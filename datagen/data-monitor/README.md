### Development

Run two processes in separate terminals:

Start the WebSocket Server:

```bash
npm run start
```

This runs `server.js` on `http://localhost:8080`, serving the WebSocket (and `build/` if present, though not needed for dev).

Start the Dev Server:

```bash
npm start-dev-server
```

This runs `react-scripts start` on `http://localhost:3000`, serving the React app with hot reloading.

- Edit `App.js`: Make changes, save, and see updates instantly at `http://localhost:3000` without rebuilding.
- Test WebSocket: Run `./send-messages` to send messages to `ws://localhost:8080`, which `App.js` will receive.

### Production

When you’re ready to deploy or test the production build:

1. Build the App

   ```bash
   npm run build
   ```

   - Generates the build/ folder.

2. Run the Server

   ```bash
   npm run start
   ```

   - Serves the app at `http://localhost:8080` using the `build/` folder.

You only need to build once before running server.js in production, not during development.
