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

import java.io.Serializable;

/**
 * This represents the various logging levels in MonDemand
 * @author Michael Lum
 */
public class Level implements Serializable {

	/**
	 * textual representation
	 */
	public final static String STRINGS[] = { "emerg", "alert", "crit",
		"error", "warning", "notice", "info", "debug", "all" };	
	
	/**
	 * all off
	 */
	public final static int OFF = -1;
	
	/**
	 * Default emergency priority value for the Logger
	 */
	public final static int EMERG = 0;

	/**
	 * Default alert priority value for the Logger
	 */
	public final static int ALERT = 1;

	/**
	 * Default critical priority value for the Logger
	 */
	public final static int CRIT = 2;

	/**
	 * Default error priority value for the Logger
	 */
	public final static int ERROR = 3;

	/**
	 * Default warning priority value for the Logger
	 */
	public final static int WARNING = 4;

	/**
	 * Default notice priority value for the Logger
	 */
	public final static int NOTICE = 5;

	/**
	 * Default info priority value for the Logger
	 */
	public final static int INFO = 6;

	/**
	 * Default debug priority value for the Logger
	 */
	public final static int DEBUG = 7;

	/**
	 * Default value to send all messages for the Logger
	 */
	public final static int ALL = 8;	
	
}
