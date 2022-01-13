/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.utility;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.TestEnvironment;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class ExecInContainerUtility {
  public static Integer execInContainer(GenericContainer<?> container, Consumer<OutputFrame> consumer, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container, consumer, StandardCharsets.UTF_8, command);
  }

  public static Integer execInContainer(GenericContainer<?> container, Consumer<OutputFrame> consumer, Charset outputCharset, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, outputCharset, command);
  }

  public static Integer execInContainer(InspectContainerResponse containerInfo, Consumer<OutputFrame> consumer,
    Charset outputCharset, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    if (!TestEnvironment.dockerExecutionDriverSupportsExec()) {
      // at time of writing, this is the expected result in CircleCI.
      throw new UnsupportedOperationException(
              "Your docker daemon is running the \"lxc\" driver, which doesn't support \"docker exec\".");
    }

    if (!isRunning(containerInfo)) {
      throw new IllegalStateException("execInContainer can only be used while the Container is running");
    }

    final String containerId = containerInfo.getId();
    final String containerName = containerInfo.getName();

    final DockerClient dockerClient = DockerClientFactory.instance().client();

    final ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
            .withAttachStdout(true).withAttachStderr(true).withCmd(command).exec();

    try (final FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, consumer);
      callback.addConsumer(OutputFrame.OutputType.STDERR, consumer);

      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }
    final Integer exitCode = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCode();
    return exitCode;
  }

  private static boolean isRunning(InspectContainerResponse containerInfo) {
    try {
      return containerInfo != null && containerInfo.getState().getRunning();
    } catch (DockerException e) {
      return false;
    }
  }
}
