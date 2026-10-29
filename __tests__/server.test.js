const request = require('supertest');
const createApp = require('../src/app');

let app = {};
const testConfig = {
    port: 5000
};
const cosQuery = "{ products(filter: {\n vendorName: \"ibm\"\n service: \"cloudantnosqldb\"\n region: \"us-south\"\n})\n {\n attributes{ \n key \n \tvalue\n }\n service\n sku\n region\n prices {\n\t\t\tunit\n USD\n effectiveDateStart\n effectiveDateEnd\n startUsageAmount\n endUsageAmount\n termLength\n }\n }\n}";

async function init() {
    return createApp.default().then((apolloApp) => {
        app = apolloApp;
        app.server = apolloApp.listen(testConfig.port);
    });
}

describe('Server', () => {
    beforeAll(async () => await init())
    afterAll(() => app.server.close())

    it('Should listen to HTTP requests', async () => {
        const userId = '0'
        await request(app)
            .post('/graphql')
            .send({
                query: cosQuery,
            })
            .set('Accept', 'application/json')
            .then((res, err) => {
                if (err) return (err)
                expect(res.status).toBe(200)
                expect(res.body && typeof res.body === 'object').toBe(true)
                expect(res.body).toHaveProperty('data')
                expect(res.body.data).toHaveProperty('products')
                return 
            })
    })

    it('liveness', async () => {
        await request(app)
            .get('/liveness')
            .then((res, err) => {
                if (err) return (err)
                expect(res.status).toBe(200)
                return 
            })
    })
})