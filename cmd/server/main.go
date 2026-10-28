// Command server runs the durable-agent HTTP server: it loads a config file,
// wires the agent kernel to a real LLM provider and the concrete tool set,
// and serves /health and /runs.
package main

import (
	"flag"
	"log"
	"net/http"

	"vino/internal/api"
)

func main() {
	cfgPath := flag.String("config", "vino.json", "path to server config file")
	envPath := flag.String("env", ".env", "path to .env file (optional)")
	flag.Parse()

	if err := api.LoadDotenv(*envPath); err != nil {
		log.Fatalf("loading .env: %v", err)
	}

	cfg, err := api.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}
	srv, err := api.NewServer(cfg)
	if err != nil {
		log.Fatalf("starting server: %v", err)
	}

	p := cfg.Providers[cfg.Default]
	log.Printf("durable-agent server listening on %s (provider %q, model %q)", cfg.Addr, cfg.Default, p.Model)
	if err := http.ListenAndServe(cfg.Addr, srv); err != nil {
		log.Fatal(err)
	}
}
