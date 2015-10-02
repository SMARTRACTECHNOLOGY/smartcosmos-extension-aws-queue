package net.smartcosmos.plugin.service.aws.queue;

/*
 * *#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
 * SMART COSMOS AWS SQS Queue Service Plugin
 * ===============================================================================
 * Copyright (C) 2013 - 2015 Smartrac Technology Fletcher, Inc.
 * ===============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.base.Preconditions;
import net.smartcosmos.Field;
import net.smartcosmos.model.queue.IQueueRequest;
import net.smartcosmos.platform.api.annotation.ServiceExtension;
import net.smartcosmos.platform.api.annotation.ServiceType;
import net.smartcosmos.platform.api.service.IQueueService;
import net.smartcosmos.platform.base.AbstractAwsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;

@ServiceExtension(serviceType = ServiceType.QUEUE)
public class AwsQueueService extends AbstractAwsService<AWSCredentials>
        implements IQueueService
{
    private static final Logger LOG = LoggerFactory.getLogger(AwsQueueService.class);

    public static final String SERVICE_PARAM_QUEUE_SERVICE_REGION = "queueServiceRegion";

    public static final String SERVICE_PARAM_QUEUE_SERVICE_QUEUE_NAME = "queueServiceQueueName";

    public static final String DEFAULT_QUEUE_NAME = "net-smartcosmos-default-queue";

    private AmazonSQSAsyncClient sqsAsyncClient;

    private boolean onlineFlag = false;

    public AwsQueueService()
    {
        super("f0cf0bfc948143c8b5721122be348169", "AWS SQS Queue Service");
    }

    @Override
    public void initialize()
    {
        super.initialize();

        if (!exists(DEFAULT_QUEUE_NAME))
        {
            create(DEFAULT_QUEUE_NAME);
        }


        String queueName = context.getConfiguration()
                .getServiceParameters()
                .get(SERVICE_PARAM_QUEUE_SERVICE_QUEUE_NAME);

        if (queueName != null & !exists(queueName))
        {
            create(queueName);
        }
        sqsAsyncClient = new AmazonSQSAsyncClient(credentials);
    }

    @Override
    protected AWSCredentials createCloudCredentials(String accessKey, String secretAccessKey)
    {
        return new BasicAWSCredentials(accessKey, secretAccessKey);
    }

    @Override
    public void create()
    {
        String queueName = context.getConfiguration()
                .getServiceParameters()
                .get(SERVICE_PARAM_QUEUE_SERVICE_QUEUE_NAME);

        if (queueName == null)
        {
            create(DEFAULT_QUEUE_NAME);
        } else
        {
            create(queueName);
        }
    }

    @Override
    public void create(String queueName)
    {
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region region = assignRegion(sqs);

        try
        {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
            String assignedUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            LOG.debug("Assigned URL for queue named {} in region {}: {}", new Object[]{queueName,
                    region.getName(),
                    assignedUrl});

            onlineFlag = true;
        } catch (AmazonClientException e)
        {
            if (e.getCause() != null && e.getCause().getClass() == UnknownHostException.class)
            {
                // Queue most definitely does not exist!
                LOG.error("AWS host is unreachable: {}", new Object[]{e.getCause().getMessage()});
            } else
            {
                throw e;
            }
        }
    }

    @Override
    public boolean exists()
    {
        String queueName = context.getConfiguration()
                .getServiceParameters()
                .get(SERVICE_PARAM_QUEUE_SERVICE_QUEUE_NAME);

        if (queueName == null)
        {
            return exists(DEFAULT_QUEUE_NAME);
        } else
        {
            return exists(queueName);
        }
    }

    @Override
    public boolean exists(String queueName)
    {
        boolean existsFlag = false;

        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region region = assignRegion(sqs);

        try
        {
            sqs.getQueueUrl(queueName);
            existsFlag = true;
            onlineFlag = true;

            LOG.info("Queue named {} in region {} exists", new Object[]{queueName, region.getName()});
        } catch (QueueDoesNotExistException e)
        {
            // Queue most definitely does not exist!
            LOG.info("Queue named {} in region {} does not exist", new Object[]{queueName, region.getName()});
        } catch (AmazonClientException e)
        {
            if (e.getCause() != null && e.getCause().getClass() == UnknownHostException.class)
            {
                // Queue most definitely does not exist!
                LOG.error("AWS host is unreachable: {}", new Object[]{e.getCause().getMessage()});
            } else
            {
                throw e;
            }
        }

        return existsFlag;
    }

    private String fetchQueueName(IQueueRequest queueRequest)
    {
        String targetQueueName = queueRequest.getQueueName();
        if (targetQueueName.equals(IQueueRequest.DEFAULT_QUEUE_NAME))
        {
            targetQueueName = context.getConfiguration()
                    .getServiceParameters()
                    .get(SERVICE_PARAM_QUEUE_SERVICE_QUEUE_NAME);
        }

        if (targetQueueName == null)
        {
            targetQueueName = DEFAULT_QUEUE_NAME;
        }
        return targetQueueName;

    }

    @Override
    public String send(final IQueueRequest queueRequest)
    {
        Preconditions.checkNotNull(queueRequest.getMessageBody(), "messageBody must not be null");
        Preconditions.checkNotNull(queueRequest.getQueueName(), "queueName must not be null");

        if (onlineFlag)
        {
            final Region region = assignRegion(sqsAsyncClient);

            String targetQueueName = fetchQueueName(queueRequest);

            String queueUrl = sqsAsyncClient.getQueueUrl(new GetQueueUrlRequest(targetQueueName)).getQueueUrl();
            LOG.debug("Queue URL for queue named {} in region {}: {}",
                    queueRequest.getQueueName(), region.getName(), queueUrl);

            SendMessageRequest request = new SendMessageRequest(queueUrl, queueRequest.getMessageBody());

            if (null != queueRequest.getMessageAttributes())
            {
                for (Map.Entry<String, String> entry : queueRequest.getMessageAttributes().entrySet())
                {
                    if (entry.getValue() != null && entry.getValue().length() > 0)
                    {
                        if (entry.getKey().equals(Field.LAST_MODIFIED_TIMESTAMP_FIELD) ||
                                entry.getKey().equals(Field.FILE_CONTENT_LENGTH))
                        {
                            request.addMessageAttributesEntry(entry.getKey(),
                                    new MessageAttributeValue()
                                            .withDataType("Number")
                                            .withStringValue(entry.getValue()));
                        } else
                        {
                            request.addMessageAttributesEntry(entry.getKey(),
                                    new MessageAttributeValue()
                                            .withDataType("String")
                                            .withStringValue(entry.getValue()));
                        }
                    }
                }
            }

            if (queueRequest.getMoniker() != null && queueRequest.getMoniker().length() > 0)
            {
                request.addMessageAttributesEntry(Field.MONIKER_FIELD,
                        new MessageAttributeValue()
                                .withDataType("String")
                                .withStringValue(queueRequest.getMoniker()));
            }

            final String enqueueUrn = "urn:uuid:" + UUID.randomUUID().toString();

            request.addMessageAttributesEntry(Field.QUEUE_URN_FIELD,
                    new MessageAttributeValue().withDataType("String.URN").withStringValue(enqueueUrn));

            request.addMessageAttributesEntry(Field.ENTITY_REFERENCE_TYPE,
                    new MessageAttributeValue()
                            .withDataType("String.EntityReferenceType")
                            .withStringValue(queueRequest.getEntityReferenceType().toString()));

            request.addMessageAttributesEntry(Field.REFERENCE_URN_FIELD,
                    new MessageAttributeValue()
                            .withDataType("String.ReferenceUrn")
                            .withStringValue(queueRequest.getReferenceUrn()));

            AwsQueueAsyncHandler asyncHandler = new AwsQueueAsyncHandler(queueRequest, region, enqueueUrn);
            sqsAsyncClient.sendMessageAsync(request, asyncHandler);
            return enqueueUrn;

        } else
        {
            throw new AmazonClientException("AWS Queue Service is not online");
        }
    }

    @Override
    public boolean isHealthy()
    {
        try
        {
            AmazonSQS sqs = new AmazonSQSClient(credentials);
            assignRegion(sqs);
            return true;
        } catch (Exception e)
        {
            return false;
        }
    }

    private Region assignRegion(AmazonSQS sqs)
    {
        String regionName = context.getConfiguration().getServiceParameters().get(SERVICE_PARAM_QUEUE_SERVICE_REGION);
        if (regionName != null)
        {
            try
            {
                Regions region = Regions.fromName(regionName);
                Region queueRegion = Region.getRegion(region);
                sqs.setRegion(queueRegion);
                LOG.debug("Using region " + regionName);
                return queueRegion;
            } catch (Exception e)
            {
                // Default to the US_EAST_1 region
                sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
                LOG.warn("Using region US_EAST_1 as a result of an exception: " + e.getMessage());
            }
        } else
        {
            // Default to the US_EAST_1 region
            sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
            LOG.info("Using default region of US_EAST_1");
        }

        return Region.getRegion(Regions.US_EAST_1);
    }
}
