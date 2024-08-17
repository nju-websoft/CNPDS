package nju.websoft.chinaopendataportal.Service;

import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import nju.websoft.chinaopendataportal.Model.Portal;

@Service
public class PortalService {

    @Value("${websoft.chinaopendataportal.portal}")
    private String portalPath;

    private Map<String, Map<String, Portal>> portals = new HashMap<>();

    public PortalService() {
    }

    @PostConstruct
    private void init() {
        try (JsonReader reader = new JsonReader(new FileReader(portalPath))) {
            Gson gson = new Gson();
            Type portalsType = new TypeToken<Map<String, Map<String, Portal>>>() {
            }.getType();
            portals = gson.fromJson(reader, portalsType);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Portal getPortalByProvinceAndCity(String province, String city) {
        return portals.get(province).get(city);
    }
}
