package run.halo.alioss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.StorageClass;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.halo.app.core.extension.attachment.Attachment;
import run.halo.app.core.extension.attachment.Attachment.AttachmentSpec;
import run.halo.app.core.extension.attachment.Constant;
import run.halo.app.core.extension.attachment.endpoint.AttachmentHandler;
import run.halo.app.extension.Metadata;
import run.halo.app.infra.utils.JsonUtils;

@Slf4j
@Extension
public class AliOssAttachmentHandler implements AttachmentHandler {

    private static final String OBJECT_KEY = "alioss.plugin.halo.run/object-key";

    @Override
    public Mono<Attachment> upload(UploadOption uploadOption) {
        var settingJson = uploadOption.configMap().getData().getOrDefault("default", "{}");
        var properties = JsonUtils.jsonToObject(settingJson, AliOssProperties.class);

        return upload(uploadOption, properties)
            .map(objectDetail -> {
                var host = properties.getBucket() + "." + properties.getEndpoint();
                var externalLink =
                    properties.getProtocol() + "://" + host + "/" + objectDetail.objectName();

                var metadata = new Metadata();
                metadata.setName(UUID.randomUUID().toString());
                metadata.setAnnotations(Map.of(
                    OBJECT_KEY, objectDetail.objectName(),
                    Constant.EXTERNAL_LINK_ANNO_KEY,
                    UriUtils.encodePath(externalLink, StandardCharsets.UTF_8)
                ));

                var objectMetadata = objectDetail.objectMetadata();
                var spec = new AttachmentSpec();
                spec.setSize(objectMetadata.getContentLength());
                spec.setDisplayName(uploadOption.file().filename());
                spec.setMediaType(objectMetadata.getContentType());

                var attachment = new Attachment();
                attachment.setMetadata(metadata);
                attachment.setSpec(spec);
                return attachment;
            });
    }

    Mono<ObjectDetail> upload(UploadOption uploadOption, AliOssProperties properties) {
        return Mono.fromCallable(() -> {
                var client =
                    new OSSClientBuilder().build(properties.getEndpoint(), properties.getAccessKey(),
                        properties.getAccessSecret());
                // build object name
                var objectName = uploadOption.file().filename();
                if (StringUtils.hasText(properties.getLocation())) {
                    objectName = properties.getLocation() + "/" + objectName;
                }

                try {
                    var pos = new PipedOutputStream();
                    var pis = new PipedInputStream(pos);
                    DataBufferUtils.write(uploadOption.file().content(), pos)
                        .subscribeOn(Schedulers.boundedElastic()).doOnComplete(() -> {
                            try {
                                pos.close();
                            } catch (IOException ioe) {
                                // close the stream quietly
                                log.warn("Failed to close output stream", ioe);
                            }
                        }).subscribe(DataBufferUtils.releaseConsumer());

                    final var bucket = properties.getBucket();
                    log.info("Uploading {} into AliOSS {}/{}/{}", uploadOption.file().filename(),
                        properties.getEndpoint(), bucket, objectName);

                    var request = new PutObjectRequest(bucket, objectName, pis);
                    var metadata = new ObjectMetadata();
                    metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
                    metadata.setObjectAcl(CannedAccessControlList.PublicRead);
                    request.setMetadata(metadata);

                    var result = client.putObject(request);
                    if (log.isDebugEnabled()) {
                        log.debug("""
                                PutObjectResult: request id: {}, version id: {}, server CRC: {}, 
                                client CRC: {}, etag: {}, response status: {}, error response: {}
                                """,
                            result.getRequestId(),
                            result.getVersionId(),
                            result.getServerCRC(),
                            result.getClientCRC(),
                            result.getETag(),
                            result.getResponse().getStatusCode(),
                            result.getResponse().getErrorResponseAsString());
                    }
                    ObjectMetadata objectMetadata = client.getObjectMetadata(bucket, objectName);
                    return new ObjectDetail(bucket, objectName, objectMetadata);
                } catch (OSSException oe) {
                    log.error("""
                        Caught an OSSException, which means your request made it to OSS, but was 
                        rejected with an error response for some reason. 
                        Error message: {}, error code: {}, request id: {}, host id: {}
                        """, oe.getErrorCode(), oe.getErrorCode(), oe.getRequestId(), oe.getHostId());
                    throw Exceptions.propagate(oe);
                } catch (ClientException ce) {
                    log.error("""
                        Caught an ClientException, which means the client encountered a serious internal problem while trying to communicate with OSS, such as not being able to access the network.
                        """);
                    throw Exceptions.propagate(ce);
                } catch (IOException ioe) {
                    throw Exceptions.propagate(ioe);
                } finally {
                    client.shutdown();
                }
            })
            .subscribeOn(Schedulers.boundedElastic());
    }

    record ObjectDetail(String bucketName, String objectName, ObjectMetadata objectMetadata) {
    }

    @Data
    static class AliOssProperties {

        private String bucket;

        private String endpoint;

        private String accessKey;

        private String accessSecret;

        private String location;

        private Protocol protocol = Protocol.https;

        private String domain;

        private String allowExtensions;

    }

    enum Protocol {
        http, https
    }

}
