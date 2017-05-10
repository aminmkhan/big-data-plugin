/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.vfs.hdfs;

import static org.pentaho.big.data.api.cluster.NamedCluster.*;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class HDFSFileSystemConfigBuilder extends FileSystemConfigBuilder {

  private static final HDFSFileSystemConfigBuilder BUILDER = new HDFSFileSystemConfigBuilder();

  /**
   * @return HdfsFileSystemConfigBuilder instance
   */
  public static HDFSFileSystemConfigBuilder getInstance() {
    return BUILDER;
  }

  /**
   * @return HDFSFileSystem
   */
  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return HDFSFileSystem.class;
  }

  public String getNamedClusterFSOption( final FileSystemOptions opts ) {
    return this.getString( opts, NAMED_CLUSTER_FS_OPTION);
  }

  public void setNamedClusterFSOption( final FileSystemOptions opts, String namedClusterName ) {
    this.setParam( opts, NAMED_CLUSTER_FS_OPTION, namedClusterName );
  }

}
