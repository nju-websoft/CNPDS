package nju.websoft.chinaopendataportal.Util;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.HandlerInterceptor;

public class RequestLimitInterceptor implements HandlerInterceptor {

    private int MAX_REQUESTS_LIMIT = 10;
    private int TIME_WINDOW_SECONDS = 60;

    private final ConcurrentMap<String, Map.Entry<Integer, Instant>> requestCounts = new ConcurrentHashMap<>();

    public void incrementRequestCount(String ip) {
        requestCounts.compute(ip, (key, entry) -> {
            if (entry == null || entry.getValue().isBefore(Instant.now().minusSeconds(TIME_WINDOW_SECONDS))) {
                return Map.entry(1, Instant.now());
            } else {
                return Map.entry(entry.getKey() + 1, entry.getValue());
            }
        });
    }

    public boolean isLimitExceeded(String ip) {
        Map.Entry<Integer, Instant> entry = requestCounts.get(ip);
        return entry != null && entry.getValue().isAfter(Instant.now().minusSeconds(TIME_WINDOW_SECONDS))
                && entry.getKey() > MAX_REQUESTS_LIMIT;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String ip = request.getRemoteAddr();
        incrementRequestCount(ip);
        if (isLimitExceeded(ip)) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return false;
        }
        return true;
    }
}