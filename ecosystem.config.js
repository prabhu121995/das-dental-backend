module.exports = {
  apps: [
    {
      name: "rcm-dental-api",
      script: "uvicorn",
      args: "app.main:app --host 0.0.0.0 --port 8000 --workers 4",
      interpreter: "none",
      autostart: true,
      autorestart: true,
      watch: false,
    },
  ],
};
