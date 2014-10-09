/* ------------------------------------------------
Copyright 2014 AT&T Intellectual Property
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ------------------------------------------- */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netdb.h>
#include <unistd.h>

#include "simple_http.h"

#define MAX_HTTP_REQUEST 64 * 1024


// perform HTTP GET and retrieve the json response
int http_get_request(endpoint addr, gs_csp_t url, gs_uint32_t* http_code, gs_sp_t json_response) {

	int sockfd;
	struct sockaddr_in servaddr;
	char request_buf[MAX_HTTP_REQUEST];
	char response_buf[MAX_HTTP_REQUEST];
	char temp_buf[MAX_HTTP_REQUEST];
	char content_type[MAX_HTTP_REQUEST];
	int n;

	char ipstr[16];
	inet_ntop(AF_INET, &addr.ip, ipstr, INET_ADDRSTRLEN);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (!sockfd) {
		fprintf(stderr, "Unable to create socket for HTTP connection\n");
		return -1;
	}

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = addr.port;
	servaddr.sin_addr.s_addr = addr.ip;

	if (connect(sockfd, (sockaddr*)&servaddr, sizeof(servaddr))) {
		fprintf(stderr, "Unable to connect to HTTP server\n");
		return -1;
	}

	// construct HTTP request
	sprintf(request_buf, "GET %s HTTP/1.0\r\n"
			"Host: %s\r\n"
			"Accept: application/json\r\n"
			"Connection: close\r\n\r\n",
			url, ipstr);

	write(sockfd, request_buf, strlen(request_buf));

	// retrive HTTP response
	char* write_pos = response_buf;
	while ((n = read(sockfd, write_pos, MAX_HTTP_REQUEST)) > 0)
		write_pos += n;
	*write_pos = '\0';
	close(sockfd);

	// split HTTP response into header and the main body
	char* header_end = strstr(response_buf, "\r\n\r\n");
	*header_end = 0;
	strcpy(json_response, header_end + 4);

	// parse HTTP header
	char* header_line = strtok(response_buf, "\r\n");
	int ver1, ver2;

	// extract http response code
	sscanf (header_line, "HTTP/%d.%d %d %s", &ver1, &ver2, http_code, temp_buf);

	// extract content-type
	*content_type = 0;
	while ((header_line = strtok(NULL, "\r\n"))) {
		if (!strncmp(header_line, "Content-Type: ", strlen("Content-Type: "))) {
			strcpy(content_type, header_line + strlen("Content-Type: "));
			if (strcmp(content_type, "application/json")) {
				printf("Invalid Content-Type %s, application/json expected\n", content_type);
				return -1;
			}
			break;		// we only care about Content-Type headers
		}
	}

	if ((*http_code == 200) && (*content_type == 0)) {
		printf("Missing Content-Type in server response, application/json expected\n");
		return -1;
	}


	return 0;
}

// perform HTTP POST request ignoring the response
int http_post_request(endpoint addr, gs_csp_t url, gs_sp_t json_request, gs_uint32_t* http_code) {
	int sockfd;
	struct sockaddr_in servaddr;
	char request_buf[MAX_HTTP_REQUEST];
	char response_buf[MAX_HTTP_REQUEST];
	char temp_buf[MAX_HTTP_REQUEST];
	int n;

	char ipstr[16];
	inet_ntop(AF_INET, &addr.ip, ipstr, INET_ADDRSTRLEN);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (!sockfd) {
		fprintf(stderr, "Unable to create socket for HTTP connection\n");
		return -1;
	}

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = addr.port;
	servaddr.sin_addr.s_addr = addr.ip;

	if (connect(sockfd, (sockaddr*)&servaddr, sizeof(servaddr))) {
		fprintf(stderr, "Unable to connect to HTTP server\n");
		return -1;
	}

	// construct HTTP request
	sprintf(request_buf, "POST %s HTTP/1.0\r\n"
			"Host: %s\r\n"
			"Content-Type: application/json\r\n"
			"Content-Length: %lu\r\n"
			"Connection: close\r\n\r\n"
			"%s",
			url, ipstr, strlen(json_request), json_request);

	write(sockfd, request_buf, strlen(request_buf));

	// retrive HTTP response
	char* write_pos = response_buf;
	while ((n = read(sockfd, write_pos, MAX_HTTP_REQUEST)) > 0)
		write_pos += n;
	*write_pos = '\0';
	close(sockfd);

	// parse HTTP header
	char* header_line = strtok(response_buf, "\r\n");
	int ver1, ver2;

	// extract http response code
	sscanf (header_line, "HTTP/%d.%d %d %s", &ver1, &ver2, http_code, temp_buf);

	return 0;
}
