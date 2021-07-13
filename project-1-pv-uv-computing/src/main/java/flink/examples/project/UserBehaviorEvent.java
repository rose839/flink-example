package flink.examples.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviorEvent {
    private Integer userID;
    private Integer itemID;
    private String category;
    private String clientIP;
    private String action;
    private Long ts;
}
