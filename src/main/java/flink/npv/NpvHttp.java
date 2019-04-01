package flink.npv;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;

/**
 * @Auther: yanglin
 * @Date: 2018/10/9 16:50
 * @Description: 趋势指标数据在clickhouse的po
 */
@Setter
@Getter
@RequiredArgsConstructor(staticName = "of")
public class NpvHttp {

    @NonNull
    long timestamp;

    @NonNull
    String srcIp;

    @NonNull
    int rspDelayTime;

}
