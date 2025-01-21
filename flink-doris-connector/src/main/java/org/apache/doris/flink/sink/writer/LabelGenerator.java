// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.writer;

import org.apache.flink.util.Preconditions;

import java.util.UUID;
import java.util.regex.Pattern;

/** Generator label for stream load. */
public class LabelGenerator {
    // doris default label regex
    private static final String LABEL_REGEX = "^[-_A-Za-z0-9:]{1,128}$";
    private static final Pattern LABEL_PATTERN = Pattern.compile(LABEL_REGEX);
    private String labelPrefix;
    private boolean enable2PC;
    private String tableIdentifier;
    private int subtaskId;

    public LabelGenerator(String labelPrefix, boolean enable2PC) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
    }

    public LabelGenerator(
            String labelPrefix, boolean enable2PC, String tableIdentifier, int subtaskId) {
        this(labelPrefix, enable2PC);
        // The label of stream load can not contain `.`
        this.tableIdentifier = tableIdentifier.replace(".", "_");
        this.subtaskId = subtaskId;
    }

    public LabelGenerator(String labelPrefix, boolean enable2PC, int subtaskId) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
        this.subtaskId = subtaskId;
    }

    public String generateTableLabel(long chkId) {
        Preconditions.checkState(tableIdentifier != null);
        String label = String.format("%s_%s_%s_%s", labelPrefix, tableIdentifier, subtaskId, chkId);

        if (!enable2PC) {
          label = label + "_" + UUID.randomUUID();
        }

        if (LABEL_PATTERN.matcher(label).matches()) {
          // The unicode table name or length exceeds the limit
          return label.length() > 128 ? label.substring(0, 128) : label;
        }

        if (enable2PC) {
          // In 2pc, replace uuid with the table name. This will cause some txns to fail to be
          // aborted when aborting.
          // Later, the label needs to be stored in the state and aborted through label
          String uuidLabel = String.format("%s_%s_%s_%s", labelPrefix, UUID.randomUUID(), subtaskId, chkId);
          return label.length() > 128 ? label.substring(0, 128) : label;
        } else {
          String uuidLabel =  String.format("%s_%s_%s_%s", labelPrefix, subtaskId, chkId, UUID.randomUUID());
          return label.length() > 128 ? label.substring(0, 128) : label;
        }
    }

    public String generateBatchLabel(String table) {
        String uuidLabel = String.format("%s_%s_%s", labelPrefix, table, UUID.randomUUID().toString());
        if (!LABEL_PATTERN.matcher(label).matches()) {
          String uuidLabel = labelPrefix + "_" + uuid;
          return label.length() > 128 ? label.substring(0, 128) : label;
        }
        return uuidLabel.length() > 128 ? uuidLabel.substring(0, 128) : uuidLabel;
    }

    public String generateCopyBatchLabel(String table, long chkId, int fileNum) {
        String uuidLabel = String.format("%s_%s_%s_%s_%s", labelPrefix, table, subtaskId, chkId, fileNum);
        return uuidLabel.length() > 128 ? uuidLabel.substring(0, 128) : uuidLabel;
    }

    public String getConcatLabelPrefix() {
        String concatPrefix = String.format("%s_%s_%s", labelPrefix, tableIdentifier, subtaskId);
        return concatPrefix.length() > 128 ? concatPrefix.substring(0, 128) : concatPrefix;
    }
}
