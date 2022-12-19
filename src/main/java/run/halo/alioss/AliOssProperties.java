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
        if (domain != null){
            if (domain.toLowerCase().startsWith("http://")){
                domain = domain.substring(7);
            } else if (domain.toLowerCase().startsWith("https://")) {
                domain = domain.substring(8);
            }
        }
        this.domain = domain;
    }
}
