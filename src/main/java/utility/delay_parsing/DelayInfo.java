package utility.delay_parsing;

public class DelayInfo {

    private Long hours;
    private Long minutes;
    private boolean hoursData;

    public DelayInfo(Long hours, Long minutes, boolean hoursData) {
        this.hours = hours;
        this.minutes = minutes;
        this.hoursData = hoursData;
    }

    public Long getHours() {
        return hours;
    }

    public Long getMinutes() {
        return minutes;
    }

    public boolean hasHoursData() {
        return hoursData;
    }
}
