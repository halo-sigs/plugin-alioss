package run.halo.alioss;

import lombok.Data;
import org.springframework.util.StringUtils;

@Data
class AliOssProperties {

    private String bucket;

    private String endpoint;

    private String accessKey;

    private String accessSecret;

    private String location;

    private Protocol protocol = Protocol.https;

    private String domain;

    private String allowExtensions;

    public String getObjectName(String filename) {
        var objectName = filename;
        if (StringUtils.hasText(getLocation())) {
            objectName = getLocation() + "/" + objectName;
        }
        return objectName;
    }

    enum Protocol {
        http, https
    }

    public void setDomain(String domain) {
        this.domain = UrlUtils.removeHttpPrefix(domain);
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = UrlUtils.removeHttpPrefix(endpoint);
    }

    public void setLocation(String location) {
        final var fileSeparator = "/";
        if (StringUtils.hasText(location)) {
            if (location.startsWith(fileSeparator)) {
                location = location.substring(1);
            }
            if (location.endsWith(fileSeparator)) {
                location = location.substring(0, location.length() - 1);
            }
        } else {
            location = "";
        }
        this.location = location;
    }
}
