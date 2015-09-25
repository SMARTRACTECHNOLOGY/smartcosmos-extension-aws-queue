package net.smartcosmos.plugin.service.aws.queue;

/*
 * *#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
 * SMART COSMOS AWS SQS Queue Service Extension
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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import net.smartcosmos.model.queue.IQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initially created by tcross on September 25, 2015.
 */
public class AwsQueueAsyncHandler<Q extends SendMessageRequest, S extends SendMessageResult>
        implements AsyncHandler<SendMessageRequest, SendMessageResult>
{

    IQueueRequest queueRequest;
    Region region;
    String enqueueUrn;

    public AwsQueueAsyncHandler(IQueueRequest queueRequest, Region region, String enqueueUrn)
    {
        this.queueRequest = queueRequest;
        this.region = region;
        this.enqueueUrn = enqueueUrn;
    }

    private static final Logger LOG = LoggerFactory.getLogger(AwsQueueAsyncHandler.class);
    @Override
    public void onError(Exception e)
    {
        LOG.error("Error sending to queue named {} in region {} for Queue URN {}: {}",
                  new Object[]{queueRequest.getQueueName(),
                               region.getName(),
                               enqueueUrn, e.getMessage()});
        e.printStackTrace();
    }


    @Override
    public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult)
    {
        LOG.info("Queue URN {} sent to queue {} in region {} successfully; queued under message ID {}",
                 new Object[]{enqueueUrn,
                              request.getQueueUrl(),
                              region.getName(),
                              sendMessageResult.getMessageId()});
    }

}
