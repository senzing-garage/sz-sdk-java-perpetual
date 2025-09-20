/**
 * This package provides the "perpetual core" implementation of the 
 * Senzing Java SDK by extending the Senzing Core SDK implementation
 * found in the {@link com.senzing.sdk.core} package.
 * 
 * <p>
 * The perpetual core implementation adds enhancements like automatic
 * handling of the following:
 * </p>
 * <ul>
 *  <li>
 *      Optional basic retry logic to retry <b>any</b> Senzing
 *      Core SDK method that fails with an {@link 
 *      SzRetryableException}.  The method may be retried one
 *      or more times with an increasing delay between retry
 *      attempts.
 *  </li>
 *  <li>
 *      Optional automatic configuration refresh so that the
 *      active configuration ID remains in sync with the 
 *      current default configuration ID.
 *  </li>
 *  <li>
 *      When automatic configuration refresh is enabled, this
 *      implementation automatically refreshes the configuration
 *      when a Senzing Core SDK method annotated as
 *      {@link SzConfigRetryable} fails with an {@link 
 *      SzException}, subsequently retrying that method if in
 *      fact the the active configuration was changed.
 *  </li>
 *  <li>
 *      Optional isolation of all Senzing Core SDK operations
 *      to an execution thread pool to enhance performance by
 *      reusing cached thread-local resources while preventing
 *      excessive memory usage that could occur when executing
 *      Senzing operations in too many threads.
 *  </li>
 *  <li>
 *      A limited {@link ExecutorService}-like interface to 
 *      enable executing multiple Senzing Core SDK operations
 *      within the execution thread pool while preventing
 *      excessive context switching.
 *  </li>
 * </ul>
 * 
 * <p>
 * Because this extends the Senzing Core SDK, it still leverages
 * the underlying native libraries provided with the Senzing 
 * product and requires the native library path settings to be
 * configured in the same way as the Senzing Core SDK.
 * </p>
 */
package com.senzing.sdk.core;
