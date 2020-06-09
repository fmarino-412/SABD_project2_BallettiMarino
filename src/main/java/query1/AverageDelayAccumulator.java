package query1;

import java.util.Date;

public class AverageDelayAccumulator {

    private Long total;
    private Long count;
    private Date startDate;
    private Date endDate;

    public AverageDelayAccumulator(Long total, Long count, Date startDate, Date endDate) {
        this.total = total;
        this.count = count;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public Long getTotal() {
        return total;
    }

    public Long getCount() {
        return count;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }
}
