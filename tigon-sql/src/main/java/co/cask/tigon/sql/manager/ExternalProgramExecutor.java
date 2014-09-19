/**
 * Copyright 2012-2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tigon.sql.manager;

import co.cask.tigon.io.Locations;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A package private class for executing an external program.
 */
public final class ExternalProgramExecutor extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalProgramExecutor.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final String name;
  private final Location executable;
  private final Location workingDir;
  private final String[] args;
  private ExecutorService executor;
  private Process process;
  private Thread shutdownThread;
  private int exitCode = -1;
  private String pid;


  public ExternalProgramExecutor(String name, Location cwd, Location executable, String...args) {
    this.name = name;
    this.workingDir = cwd;
    this.executable = executable;
    this.args = args;
  }

  public ExternalProgramExecutor(String name, Location executable, String...args) {
    this.name = name;
    this.workingDir = Locations.getParent(executable);
    this.executable = executable;
    this.args = args;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s %s", name, workingDir.toURI().getPath(), executable.toURI().getPath(),
                         Arrays.toString(args));
  }

  @Override
  protected void startUp() throws Exception {
    // We need two threads.
    // One thread for keep reading from input, write to process stdout and read from stdin.
    // The other for keep reading stderr and log.
    executor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("process-" + name + "-%d").build());

    // the Shutdown thread is to time the shutdown and kill the process if it timeout.
    shutdownThread = createShutdownThread();

    List<String> cmd = ImmutableList.<String>builder().add(executable.toURI().getPath()).add(args).build();
    process = new ProcessBuilder(cmd).directory(new File(workingDir.toURI().getPath())).start();
    executor.execute(createProcessRunnable(process));
    executor.execute(createLogRunnable(process));

    //Get Process ID of the initiated process
    try {
      Field f = process.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      pid = Integer.toString(f.getInt(process));
    } catch (Throwable e) {
      LOG.info("Cannot retrieve process ID of RTS process");
    }
  }

  @Override
  protected void run() throws Exception {
    // Simply wait for the process to complete.
    // Trigger shutdown would trigger closing of in/out streams of the process,
    // which if the process implemented correctly, should complete.
    exitCode = process.waitFor();
    if (exitCode != 0) {
      LOG.error("Process {} exit with exit code {}", this, exitCode);
    }
  }

  /**
   * Return the exit code of the process.
   */
  public int getExitCode() {
    return exitCode;
  }

  @Override
  protected void triggerShutdown() {
    executor.shutdownNow();
    shutdownThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    shutdownThread.interrupt();
    shutdownThread.join();
  }

  private Thread createShutdownThread() {
    Thread t = new Thread("shutdown-" + name) {
      @Override
      public void run() {
        try {
          // Wait for at most SHUTDOWN_TIME and kill the process
          TimeUnit.SECONDS.sleep(SHUTDOWN_TIMEOUT_SECONDS);
          LOG.warn("Process {} took too long to stop. Killing it.", this);
          if (name.toLowerCase().contains("rts")) {
            // RTS process changes its process group ID to its own pid and then spawns child processes
            // Killing that process group to kill the RTS process and all its descendants
            try {
              Process killRTS = new ProcessBuilder("kill", "-9", "-" + pid).start();
            } catch (IOException e) {
              LOG.warn("Failed to shutdown RTS process");
            }
          } else {
            process.destroy();
          }
        } catch (InterruptedException e) {
          // If interrupted, meaning the process has been shutdown nicely.
        }
      }
    };
    t.setDaemon(true);
    return t;
  }

  private Runnable createProcessRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.UTF_8));
        try {

          String line = reader.readLine();
          while (!Thread.currentThread().isInterrupted() && line != null) {
            LOG.info(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          LOG.error("Exception when reading from stdout stream for {}.", ExternalProgramExecutor.this);
        } finally {
          Closeables.closeQuietly(reader);
        }
        LOG.info("Process completed {}.", ExternalProgramExecutor.this);
      }
    };
  }

  private Runnable createLogRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), Charsets.UTF_8));
        try {
          String line = reader.readLine();
          while (!Thread.currentThread().isInterrupted() && line != null) {
            LOG.info(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          LOG.error("Exception when reading from stderr stream for {}.", ExternalProgramExecutor.this);
        } finally {
          Closeables.closeQuietly(reader);
        }
      }
    };
  }
}
