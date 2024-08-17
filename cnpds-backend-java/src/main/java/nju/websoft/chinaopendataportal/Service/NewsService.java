package nju.websoft.chinaopendataportal.Service;

import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import nju.websoft.chinaopendataportal.Model.News;

@Service
public class NewsService {
    @Value("${websoft.chinaopendataportal.news}")
    private String newsPath;

    private List<News> news;

    public NewsService() {
    }

    @PostConstruct
    private void init() {
        try (JsonReader reader = new JsonReader(new FileReader(newsPath))) {
            Gson gson = new Gson();
            Type portalsType = new TypeToken<List<News>>() {
            }.getType();
            news = gson.fromJson(reader, portalsType);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<News> getTop5News() {
        return news.subList(0, Math.min(5, news.size()));
    }

    public List<News> getAllNews() {
        return news;
    }
}
