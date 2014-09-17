/*======================================================================*
 * Copyright (c) 2008, Yahoo! Inc. All rights reserved.                 *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License.  Unless required    *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.mondemand;

/**
 * Class for throwing exceptions from transport implementations
 * @author Michael Lum
 *
 */
public class TransportException extends Exception {

  private static final long serialVersionUID = -4729328172963540215L;

  public TransportException() {
    super();
  }

  public TransportException(String message) {
    super(message);
  }

  public TransportException(Exception e) {
    super(e);
  }

  public TransportException(String message, Exception e) {
    super(message, e);
  }
}
