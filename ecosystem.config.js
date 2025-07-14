// ecosystem.config.js
module.exports = {
    apps: [
        {
            name: "olimi-whatsapp-api",
            script: "dist/main.js",
            instances: 1, // you can set to "max" for cluster mode if CPU-bound
            exec_mode: "fork",
            env: {
                NODE_ENV: "production"
            }
        }
    ]
};
