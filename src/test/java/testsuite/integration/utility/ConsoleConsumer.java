/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
 */

package testsuite.integration.utility;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class ConsoleConsumer extends BaseConsumer<org.testcontainers.containers.output.Slf4jLogConsumer> {

  private boolean separateOutputStreams;

  public ConsoleConsumer() {
    this(false);
  }

  public ConsoleConsumer(boolean separateOutputStreams) {
    this.separateOutputStreams = separateOutputStreams;
  }

  public ConsoleConsumer withSeparateOutputStreams() {
    this.separateOutputStreams = true;
    return this;
  }

  @Override
  public void accept(OutputFrame outputFrame) {
    final OutputFrame.OutputType outputType = outputFrame.getType();

    final String utf8String = outputFrame.getUtf8String();

    switch (outputType) {
      case END:
        break;
      case STDOUT:
        System.out.print(utf8String);
        break;
      case STDERR:
        if (separateOutputStreams) {
          System.err.print(utf8String);
        } else {
          System.out.print(utf8String);
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected outputType " + outputType);
    }
  }
}
