package com.salesforce.multicloudj.docstore.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    class Player {
        private String pName;
        private int i;
        private float f;
        private boolean b;
        private String s;
    }