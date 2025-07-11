package com.salesforce.multicloudj.docstore.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class HighScore {
    private String Game;        
    private String Player;       
    private int Score;          // Numeric score value
    private String Time;        // Timestamp string (e.g., "2024-03-13")
    private boolean WithGlitch; // Boolean flag for filtering
} 