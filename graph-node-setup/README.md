## Run a graph node
`docker-compose.yml` is already configured for a local setup. A single command is required to run a graph node (make sure you are inside graph-node-setup folder):
```sh
docker-compose up -d
```

As we launched the local graph node we can proceed with a usual subgraph deployment flow. Let's deploy a well known gravatar subgraph as an example.

## Deploy example
```sh
# 1. Clone Gravatar subgraph
git clone git@github.com:graphprotocol/example-subgraph.git gravatar-subgraph

# 2. Move to the cloned folder
cd gravatar-subgraph

# 3. Install dependencies
yarn install

# 4. Generate classes for the smart contract and events used in the subgraph
npm run codegen

# 5. Create and deploy subgraph
npm run create-local
npm run deploy-local

# Playground for the deployed subgraph is available at: http://127.0.0.1:8000/subgraphs/name/example/graphql
```
