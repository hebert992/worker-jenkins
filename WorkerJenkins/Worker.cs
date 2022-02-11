using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Api;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Text;
using Newtonsoft.Json;
using WorkerJenkins.Model;

namespace WorkerJenkins
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ConfigSub _configSub;
        private SubscriberServiceApiClient subscriberService;

        public Worker(ILogger<Worker> logger, ConfigSub configSub)
        {
            _logger    = logger;
            _configSub = configSub;

        }

        public HttpStatusCode GetHttpResponse(string url)
        {
            var client = new HttpClient();
            HttpResponseMessage result = client.GetAsync(url).Result;
            return result.StatusCode;



        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {


                _logger.LogInformation($"Iniciando servicio....");
                subscriberService = await SubscriberServiceApiClient.CreateAsync();
                SubscriptionName subscriptionName = new SubscriptionName(_configSub.projectId, _configSub.subId);
                TopicName        topicName        = new TopicName(_configSub.projectId, _configSub.topicId);
                var              sub              = subscriberService.GetSubscription(subscriptionName);
                if (sub != null)
                {
                    _logger.LogInformation($"Borrancho sub");
                    subscriberService.DeleteSubscription(subscriptionName);
                }


                subscriberService.CreateSubscription(subscriptionName, topicName, pushConfig: null,
                    ackDeadlineSeconds: 60);
                SubscriberClient subscriber = await SubscriberClient.CreateAsync(subscriptionName);
                _logger.LogInformation($"Esperando creancion...");
                Thread.Sleep(5000);
                // Pull messages from the subscription using SubscriberClient.
                _logger.LogInformation($"Iniciando escucha");
                await subscriber.StartAsync(async (msg, stoppingToken) =>
                {
                    _logger.LogInformation($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
                    dynamic obj = JObject.Parse(msg.Data.ToStringUtf8());
                    string  url = obj.url;
                    _logger.LogInformation($"url: '{url}'");
                    HttpClient          client = new HttpClient();
                    var                 data   = new StringContent(obj.body.ToString(), System.Text.Encoding.UTF8, "application/json"); ;
                    JObject                    converted   = JsonConvert.DeserializeObject<JObject>(obj.header.ToString());
                    foreach (KeyValuePair<string, JToken> keyValuePair in converted)
                    {
                        try
                        {
                            client.DefaultRequestHeaders.Add(keyValuePair.Key,keyValuePair.Value.ToString()); 
                        }catch(Exception ex){}
                        
                        _logger.LogInformation($"key: {keyValuePair.Key}: value: {keyValuePair.Value.ToString()} ");
                    }
                    
                    HttpResponseMessage        result      = await client.PostAsync(url,data);
                    return await Task.FromResult(SubscriberClient.Reply.Ack);
                });
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex.ToString());
            }
        }
    }
}