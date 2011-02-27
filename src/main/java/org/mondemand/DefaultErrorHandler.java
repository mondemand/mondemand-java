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
 * This implements the ErrorHandler interface and provides the default
 * handler for MonDemand errors, which writes errors to standard error.
 * The user can override this using the MonDemand API, if they wanted to
 * log errors another way.
 * @author Michael Lum
 */
public class DefaultErrorHandler implements ErrorHandler {

  /**
   * Writes MonDemand errors to standard error.
   */
  public void handleError(String error) {
    if(error != null) {
      System.err.println("MonDemand error: " + error);
    }

  }

  /**
   * Writes MonDemand errors that have an associated exception to
   * standard error.
   */
  public void handleError(String error, Exception e) {
    if(error != null) {
      System.err.println("MonDemand error: " + error);
    }
    if(e != null ) {
      e.printStackTrace(System.err);
    }

  }

}
