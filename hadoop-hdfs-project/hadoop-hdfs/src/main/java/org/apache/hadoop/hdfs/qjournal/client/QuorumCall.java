/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;
  
  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  
  private static final int  JOURNLANODE_FULLGC_TIME_THRESHOLD=6;
  
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>();
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      });
    }
    return qr;
  }
  
  private QuorumCall() {
    // Only instantiated from factory method above
  }
//
   static class StopWatch{
      //开始时间
      long start=0L;
      //统计时间流逝的时间
     // long elapse=0L;
      public StopWatch() {

      }

    /**
     * 开始计时
     * @return
     */
    public StopWatch start() {
      start=System.currentTimeMillis();
      return this;
    }
    /**
     * 计算流逝的时间
     * @return
     */
    public long getElapse() {
      return System.currentTimeMillis()  - start;
    }

  }

  /**
   * Wait for the quorum to achieve a certain number of responses.
   * 
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
      throws InterruptedException, TimeoutException {
    // minResponses = 5，
    // minSuccesses = 3，
    // maxExceptions = 3
    // millis = 20s，必须在20s内方法返回，要不然就是有2个journalnodes写入成功了，要不然就是写入失败
    /**
     *
     * 获取当前时间： 16:00:00
     */
    long st = Time.monotonicNow();
    /**
     * 计算日志打印的时间：16:00:00 + 6 =16:00:06
     */
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);
    /**
     * 计算超时时间点：16:00:20
     */
    long et = st + millis;

   // StopWatch stopWatch = new StopWatch();
    while (true) {//多次尝试的意思
    //  stopWatch.start();
    	
      checkAssertionErrors();
      /**
       *
       * 思路：
       * 写journalnode 可以，无论写成功和失败，你都跟我个回应。
       *
       */

      // 第一种情况：写入成功的 + 写入失败的 >= 5，5个journalnodes不管是成功还是失败，都给你返回了一个响应
      //countResponses用来统计 journalnode返回来响应点个数

      if (minResponses > 0 && countResponses() >= minResponses) return;

      // 第二种情况：写入成功的 >= 3，只要有3个journalnodes是写入成功的，也立马返回
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;

      // 第三种情况：写入失败的 >=3，只要有3个journalnodes是写入失败的，也可以立马返回
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;



      //假设这儿namenode


      /**
       * 第一次进来：获取当前时间：16:00:01
       *
       *
       * 第二次进来：获取当前时间：16:00:01+5+1秒 = 16:00:07
       *
       *
       * 16:00:24
        *
       */
      long now = Time.monotonicNow();


      /**
       * 第一次进来：判断是否已经到了打印日志的时间：16:00:01 > 16:00:06 还没到打印日志的时间。
       *
       * 第二次进来：判断是否已经到了打印日志的时间：16:00:07 > 16:00:06 到打印日志的时间。
       */
      if (now > nextLogTime) {
        //计算已经等待了多久  16:00:07 -  16:00:00 =7秒
        long waited = now - st;
        //打印日志
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        //如果等待的时间超过了
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);//warn
        } else {
          //如果没有超过，那么就打印info日志
          QuorumJournalManager.LOG.info(msg);
        }
        //重新计算下一次日志打印的时间：16:00:07 + 1 =16:00:08
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }

      /**
       * 1） 第一次进来：计算还剩多少时间 16:00:20 - 16:00:01 = 19秒
       *
       * 2） 第二次进来：计算还剩多少时间。16:00:20 - 16:00:07 = 13秒
       *
       *
       * 第N次进来：计算还剩多少时间。16:00:20 - 16:00:21 = -1
       */
      long rem = et - now;
      /**
       * 第一次进来：19 <= 0 不成立，所以不超时退出

       * 第二次进来：13 <= 0 不成立，所以不超时退出
       *
       * 剩余的时间-1
       * 如何提升这个稳定性？
       */
      if(rem <= 0){ //说明已经超时了
        throw  new TimeoutException();
      }

//
//      if (rem <= 0) {//namenod写journalnode如果写超时了
//           //  就要判读是否发生了full gc
//        //23
//        long elapse = stopWatch.getElapse();
//        // 我到代码有点不好，就是直接硬编码，给了一个5。
//       // 其实我这儿直接给了5秒，其实是不妥的，这个值应该是要把设置成为一个可配置的值。
//        //硬编码，其实是不太对的，正常编码我们应该把这个值设置为一个
//        //可配置的值，应该是在配置文件里面可以配置。
//        if(elapse > 5){//说明代码肯定发生了full gc
//         // 重置超时时间
//         // et=16:00:20+23=16:00:43
//          et = et + elapse;
//        }else{
//         // 说明纯碎就是journalnode超时了。
//          // while wait
//          throw new TimeoutException();
//        }
//
//      }

      //@@@@@@@@@@@@
  //    stopWatch.start();
      /**
       * 1） 第一次进来：Math.min(19,16:00:06 -16:00:01) => 5
       *
       * 2） 第二次进来：Math.min(13,16:00:08 -16:00:07) => 1
       */
      rem = Math.min(rem, nextLogTime - now);
      /**
       * 1） 第一次进来：Math.max(5,1) => 5
       *
       * 2） 第二次进来：Match.max(1,1) => 1
       */
      rem = Math.max(rem, 1);

      /**
       * 1） 第一次进来：wait 5秒
       *
       * 2） 第二次进来：wait 1秒
       */
      wait(rem);//5

      /**
       *
       */


      //@@@@@@@@@@@@
  //    long elapse = stopWatch.getElapse();
//
//      if(elapse - rem > 5){
//        //重新计算下一次的超时时间
//        et = et + (elapse - rem);
//      }

    }
    
    
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   * 
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing throuhg.
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }
  
  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }
  
  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }
  
  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }
  
  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}
