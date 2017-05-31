/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.pentaho.big.data.bundles.impl.shim.hbase;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.HadoopException;

import org.apache.hadoop.gateway.shell.hbase.HBase;
import org.apache.hadoop.gateway.shell.hbase.table.Table;
import org.apache.hadoop.gateway.shell.hbase.table.TableList;
import org.apache.hadoop.gateway.shell.hbase.SystemVersion;

public class HBaseKnoxPrototypeTest {
  private Hadoop session;
  private static final String gatewayUrl = "https://gateway.com:8443/gateway/HDP25SecPwRgr";
  private static final String gatewayUser = "devuser";
  private static final String gatewayPassword = "password";

  public HBaseKnoxPrototypeTest(Hadoop session ) {
    this.session = session;
  }

  protected void hbaseVersion() {
    org.apache.hadoop.gateway.shell.hbase.SystemVersion.Response response = null;
    String contents = null;

    try {
      response = HBase.session( session ).systemVersion().now();
      contents = new String( response.getBytes() );
    } catch ( IOException e ) {
      e.printStackTrace();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    System.out.println( "HBase Version (status code): " + response.getStatusCode() );
    System.out.println( "Contents:\n" + contents );

  }

  protected void hbaseTableList() {
    org.apache.hadoop.gateway.shell.hbase.table.TableList.Response response = null;
    String contents = null;

    try {
      response = HBase.session(session).table().list().now();
      contents = new String( response.getBytes() );
    } catch ( IOException e ) {
      e.printStackTrace();
    } catch ( Exception e ) {
      e.printStackTrace();
    }

    System.out.println( "HBase Table List (status code): " + response.getStatusCode() );
    System.out.println( "Contents:\n" + contents );
  }

  public static void main( String[] args ) {
    Hadoop session = null;

    try {
      session = Hadoop.login( gatewayUrl, gatewayUser, gatewayPassword );
    } catch ( URISyntaxException e ) {
      e.printStackTrace();
    }

    HBaseKnoxPrototypeTest hbase = new HBaseKnoxPrototypeTest( session );

    hbase.hbaseVersion();
    hbase.hbaseTableList();
  }
}
