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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.base.Predicate;

/**
 * 
 * NameNodeResourceChecker provides a method -
 * <code>hasAvailableDiskSpace</code> - which will return true if and only if
 * the NameNode has disk space available on all required volumes, and any volume
 * which is configured to be redundant. Volumes containing file system edits dirs
 * are added by default, and arbitrary extra volumes may be configured as well.
 */
@InterfaceAudience.Private
public class NameNodeResourceChecker {
  private static final Log LOG = LogFactory.getLog(NameNodeResourceChecker.class.getName());

  // Space (in bytes) reserved per volume.
  private final long duReserved;

  private final Configuration conf;
  private Map<String, CheckedVolume> volumes;
  private int minimumRedundantVolumes;
  
  @VisibleForTesting
  class CheckedVolume implements CheckableNameNodeResource {
    private DF df;
    private boolean required;
    private String volume;
    
    public CheckedVolume(File dirToCheck, boolean required)
        throws IOException {
      df = new DF(dirToCheck, conf);
      this.required = required;
      volume = df.getFilesystem();
    }
    
    public String getVolume() {
      return volume;
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }

    /**
     * 这里面出来的这些告警大家要注意，以后遇到了
     * 这些告警，就要知道是遇到了什么问题？
     * @return
     */
    @Override
    public boolean isResourceAvailable() {
      //TODO 获取当前目录的空间大小
      long availableSpace = df.getAvailable();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Space available on volume '" + volume + "' is "
            + availableSpace);
      }
      //TODO 如果空间大小小于  100M  就返回false
      if (availableSpace < duReserved) {
    	  //ERROR
        LOG.warn("Space available on volume '" + volume + "' is "
            + availableSpace +
            ", which is below the configured reserved amount " + duReserved);

        /**
         * NameNode:有可能误操作，跑了一个任务，这个任务生成了很多数据。
         *
         */

        return false;
      } else {
        return true;
      }
    }
    
    @Override
    public String toString() {
      return "volume: " + volume + " required: " + required +
          " resource available: " + isResourceAvailable();
    }
  }

  /**
   * Create a NameNodeResourceChecker, which will check the edits dirs and any
   * additional dirs to check set in <code>conf</code>.
   */
  public NameNodeResourceChecker(Configuration conf) throws IOException {
    this.conf = conf;
    volumes = new HashMap<String, CheckedVolume>();
    

    //namenode服务器上面，肯定会有很多个目录？ 到底我们需要监控哪些目录呢？

    //TODO 阈值
    duReserved = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);
    
    Collection<URI> extraCheckedVolumes = Util.stringCollectionAsURIs(conf
        .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY));
    
    Collection<URI> localEditDirs = Collections2.filter(
        FSNamesystem.getNamespaceEditsDirs(conf),
        new Predicate<URI>() {
          @Override
          public boolean apply(URI input) {
            if (input.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
              return true;
            }
            return false;
          }
        });

    // Add all the local edits dirs, marking some as required if they are
    // configured as such.
    //TODO 添加需要监控的磁盘
    // localEditDirs -》 hdfs-site.xml  core-site.xml
    for (URI editsDirToCheck : localEditDirs) {
      addDirToCheck(editsDirToCheck,
          FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(
              editsDirToCheck));
    }

    // All extra checked volumes are marked "required"
    for (URI extraDirToCheck : extraCheckedVolumes) {
      addDirToCheck(extraDirToCheck, true);
    }
    
    minimumRedundantVolumes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
  }

  /**
   * Add the volume of the passed-in directory to the list of volumes to check.
   * If <code>required</code> is true, and this volume is already present, but
   * is marked redundant, it will be marked required. If the volume is already
   * present but marked required then this method is a no-op.
   * 
   * @param directoryToCheck
   *          The directory whose volume will be checked for available space.
   */
  private void addDirToCheck(URI directoryToCheck, boolean required)
      throws IOException {
    File dir = new File(directoryToCheck.getPath());
    if (!dir.exists()) {
      throw new IOException("Missing directory "+dir.getAbsolutePath());
    }
    //一个目录就是一个CheckedVolume
    CheckedVolume newVolume = new CheckedVolume(dir, required);
    //这个对象其实就是一个目录
    CheckedVolume volume = volumes.get(newVolume.getVolume());
    if (volume == null || !volume.isRequired()) {
      //volumes里面会有多个路径（目录）
      volumes.put(newVolume.getVolume(), newVolume);
    }
  }

  /**
   * Return true if disk space is available on at least one of the configured
   * redundant volumes, and all of the configured required volumes.
   * 
   * @return True if the configured amount of disk space is available on at
   *         least one redundant volume and all of the required volumes, false
   *         otherwise.
   *   false
   */
  public boolean hasAvailableDiskSpace() {
	      //关键调用的是这儿的代码
     //      //volumes 里面存放的就是需要检查的目录
    //如果资源不够，返回值是false
    return NameNodeResourcePolicy.areResourcesAvailable(volumes.values(),
        minimumRedundantVolumes);
  }

  /**
   * Return the set of directories which are low on space.
   * 
   * @return the set of directories whose free space is below the threshold.
   */
  @VisibleForTesting
  Collection<String> getVolumesLowOnSpace() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to check the following volumes disk space: " + volumes);
    }
    Collection<String> lowVolumes = new ArrayList<String>();
    for (CheckedVolume volume : volumes.values()) {
      lowVolumes.add(volume.getVolume());
    }
    return lowVolumes;
  }
  
  @VisibleForTesting
  void setVolumes(Map<String, CheckedVolume> volumes) {
    this.volumes = volumes;
  }
  
  @VisibleForTesting
  void setMinimumReduntdantVolumes(int minimumRedundantVolumes) {
    this.minimumRedundantVolumes = minimumRedundantVolumes;
  }
}