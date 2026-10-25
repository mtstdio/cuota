package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
	"cuota-server/scheduler"
	"cuota-server/store"
	"cuota-server/web"
)

func main() {
	configPath := flag.String("c", "config.yaml", "配置文件路径")
	flag.Parse()

	log.Println("==================================================")
	log.Println("           Cuota - Codex 配额管家服务启动          ")
	log.Println("==================================================")

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("[Main] 加载配置失败: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 初始化各核心子系统
	st, err := store.NewStore(cfg.Storage.StatePath)
	if err != nil {
		log.Printf("[Main] 警告: 初始化存储失败 (%v)，降级为内存运行", err)
	}

	cpaClient := cpa.NewClient(cfg.CPA)
	quotaFetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	probeController := probe.NewController(cfg.Probe)
	schedulerManager := scheduler.NewManager(cfg, cpaClient, quotaFetcher, probeController, st)
	webServer := web.NewServer(cfg.Server, schedulerManager)

	// 启动后台调度与配额管理引擎
	go schedulerManager.Start(ctx)

	// 启动 Web 看板与 REST API 服务
	if err := webServer.Start(ctx); err != nil {
		log.Printf("[Main] Web 服务退出: %v", err)
	}

	log.Println("[Main] 服务已平稳停止。")
}
