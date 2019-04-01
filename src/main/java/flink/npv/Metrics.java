package flink.npv;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * @Auther: yanglin
 * @Date: 2018/10/9 16:50
 * @Description: 趋势指标数据在clickhouse的po
 */
@Setter
@Getter
public class Metrics {
    int good;
    int total;


}
