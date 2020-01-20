package trades;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Trade {
    private String tradeReference;
    private String accountNumber;
    private String stockCode;
    private Double quantity;
    private String currency;
    private Double price;
    private String broker;
    private Double amount;
    private Timestamp receivedTimestamp;
}
