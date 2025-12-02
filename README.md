# Cloud Pricing API

<a href="https://www.infracost.io/community-chat"><img alt="Community Slack channel" src="https://img.shields.io/badge/chat-Slack-%234a154b"/></a> <a href="https://hub.docker.com/r/infracost/cloud-pricing-api/tags"><img alt="Docker Image" src="https://img.shields.io/badge/docker-passing-brightgreen"/></a> <a href="https://infracost.io/docs/cloud_pricing_api/overview"><img alt="Docs" src="https://img.shields.io/badge/docs-blue"/></a> <a href="https://twitter.com/intent/tweet?text=Open%20source%20GraphQL%20API%20for%20cloud%20pricing.%20Contains%20over%203M%20public%20prices%20from%20AWS%2C%20Azure%20and%20GCP!&url=https://github.com/infracost/cloud-pricing-api&hashtags=cloud,price,aws,azure,gcp"><img alt="Tweet" src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social"/></a>

The Cloud Pricing API is a GraphQL-based API that includes all public prices from AWS, Azure and Google; it contains over **3 million prices!** The prices are automatically updated via a weekly job. This API is used by the [Infracost CLI](https://github.com/infracost/infracost), thus you do not need to interact with it directly, however, you can also use it independently.

## Usage

Infracost runs a hosted version of this API that you can use if you prefer that:
1. Register for an API key by [downloading infracost](https://www.infracost.io/docs/#quick-start) and running `infracost auth login`.
2. If you'd like to use the API independently, pass the above API key using the `X-Api-Key: xxxx` HTTP header when calling [https://pricing.api.infracost.io/graphql](https://pricing.api.infracost.io/graphql). The following example `curl` fetches the latest price for an AWS EC2 m3.large instance in us-east-1. More examples can be found in `./examples/queries`.

    **Example request**:
    ```sh
    curl https://pricing.api.infracost.io/graphql \
      -X POST \
      -H 'X-Api-Key: YOUR_API_KEY_HERE' \
      -H 'Content-Type: application/json' \
      --data '
      {"query": "{ products(filter: {vendorName: \"aws\", service: \"AmazonEC2\", region: \"us-east-1\", attributeFilters: [{key: \"instanceType\", value: \"m3.large\"}, {key: \"operatingSystem\", value: \"Linux\"}, {key: \"tenancy\", value: \"Shared\"}, {key: \"capacitystatus\", value: \"Used\"}, {key: \"preInstalledSw\", value: \"NA\"}]}) { prices(filter: {purchaseOption: \"on_demand\"}) { USD } } } "}
      '
    ```

    **Example response**:
    ```sh
    {"data":{"products":[{"prices":[{"USD":"0.1330000000"}]}]}}
    ```
    
    The [GraphQL Playground](https://pricing.api.infracost.io/graphql) can also be used with something like the [modheader](https://bewisse.com/modheader/) browser extension so you can set the custom HTTP header `X-Api-Key` to your Infracost API key.

## Architecture

The following diagram shows an overview of the architecture.

![Deployment overview](.github/assets/deployment_overview.png "Deployment overview")

The pricing DB dump is downloaded from Infracost's API as that simplifies the task of keeping prices up-to-date. We have created one job that you can run once a week to download the latest prices. This provides you with:
1. **Fast updates**: our aim is to enable you to deploy this service in less than 15mins. Some cloud vendors paginates API calls to 100 resources at a time, and making too many requests result in errors; fetching prices directly from them takes more than an hour.
2. **Complete updates**: We run [integration tests](https://github.com/infracost/infracost/actions) to ensure that the CLI is using the correct prices. In the past, there have been cases when cloud providers have tweaked their pricing API data that caused direct downloads to fail. With this method, we check the pricing data passes our integration tests before publishing them, and everyone automatically gets the entire up-to-date data. The aim is reduce the risk of failed or partial updates.

## Deployment

It should take around 15 mins to deploy the Cloud Pricing API. Two deployment methods are supported:
1. If you have a Kubernetes cluster, we recommend using [our Helm Chart](https://github.com/infracost/helm-charts/tree/master/charts/cloud-pricing-api).
2. If you prefer to deploy in a VM, we recommend using [Docker compose](#docker-compose).

The Cloud Pricing API includes an unauthenticated `/health` path that is used by the Helm chart and Docker compose deployments.

The PostgreSQL DB is run on a single container/pod by default, which should be fine if your high-availability requirements allow for a few second downtime on container/pod restarts. No critical data is stored in the DB and the DB can be quickly recreated in the unlikely event of data corruption issues. Managed databases, such as a small AWS RDS or Azure Database for PostgreSQL, can also be used (PostgreSQL version >= 13). Since the pricing data can be quickly populated by running the update job, you can probably start without a backup strategy.

### Helm chart

See [our Helm Chart](https://github.com/infracost/helm-charts/tree/master/charts/cloud-pricing-api) for details.

### Docker compose

#### Prerequisites

* Docker Engine 17.09.0+

#### Steps

1. Clone the repo:

    ```sh
    git clone https://github.com/infracost/cloud-pricing-api.git
    cd cloud-pricing-api
    ```

2. Generate a 32 character API token that your Infracost CLI users will use to authenticate when calling your self-hosted Cloud Pricing API. If you ever need to rotate the API key, you can simply update this environment variable and restart the application.

    ```sh
    export SELF_HOSTED_INFRACOST_API_KEY=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
    echo "SELF_HOSTED_INFRACOST_API_KEY=$SELF_HOSTED_INFRACOST_API_KEY"
    ```

3. Add a `.env` file with the following content:

    ```sh
    # The API key you generated in step 2, used to authenticate Infracost CLI users with your self-hosted Cloud Pricing API.
    SELF_HOSTED_INFRACOST_API_KEY=<API Key from Step 2>
    ```

4. Run `docker-compose run init_job`. This will start a PostgreSQL DB container and an init container that loads the pricing data. The init container will take a few minutes and exit after the Docker compose logs show `Completed: loading data into DB`.

5. Run `docker-compose up api`. This will start the Cloud Pricing API.

6. Prices can be kept up-to-date by running the update job once a week, for example from cron:

    ```sh
    # Add a weekly cron job to update the pricing data. The cron entry should look something like:
    0 4 * * SUN docker-compose run --rm update_job npm run job:update >> /var/log/cron.log 2>&1
    ```

7. When using the CLI locally, run the following two required commands to point your CLI to your self-hosted Cloud Pricing API:

    ```sh
    infracost configure set pricing_api_endpoint http://localhost:4000
    infracost configure set api_key $SELF_HOSTED_INFRACOST_API_KEY
    
    infracost breakdown --path /path/to/code
    ```

8. In CI/CD systems, set the following two required environment variables:

    ```sh
    export INFRACOST_PRICING_API_ENDPOINT=https://endpoint
    export INFRACOST_API_KEY=$SELF_HOSTED_INFRACOST_API_KEY
    ```

9. The home page for the Cloud Pricing API, [http://localhost:4000](http://localhost:4000), shows if prices are up-to-date and some statistics.

![Stats page](.github/assets/stats_page.png "Stats page")

We recommend you setup a subdomain (and TLS certificate) to expose your self-hosted Cloud Pricing API to your Infracost CLI users.

You can also access the GraphQL Playground at [http://localhost:4000/graphql](http://localhost:4000/graphql) using something like the [modheader](https://bewisse.com/modheader/) browser extension so you can set the custom HTTP header `X-Api-Key` to your `SELF_HOSTED_INFRACOST_API_KEY`.

To upgrade to the latest version, run `docker-compose pull` followed by `docker-compose up`.

The environment variable `DISABLE_TELEMETRY` can be set to `true` to opt-out of telemetry.

### Using a self-signed certificate

See [the Infracost docs](https://www.infracost.io/docs/cloud_pricing_api/self_hosted/#using-a-self-signed-certificate) for how to configure Infracost CLI to work with a self-hosted Cloud Pricing API that uses a self-signed certificate.

## Troubleshooting

Please see [this section](https://www.infracost.io/docs/cloud_pricing_api/self_hosted/#troubleshooting) and join our [community Slack channel](https://www.infracost.io/community-chat), we'll help you very quickly 😄

## Contributing

Issues and pull requests are welcome! For development details, see the [contributing guide](CONTRIBUTING.md). For major changes, including interface changes, please open an issue first to discuss what you would like to change. [Join our community Slack channel](https://www.infracost.io/community-chat), we are a friendly bunch and happy to help you get started :)

Our team is currently focused on supporting AWS, Azure and GCP so we prefer not to work on adding new cloud vendors at this time. Before we accept contributors for new cloud providers, we would like to move towards a provider model where community-supported cloud vendors could also be added (similar to Terraform providers). If you'd like to prototype the provider model, please let us know!

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)
