package main

import (
	"os"

	"github.com/alexsward/steel/store"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	if err := app().Run(os.Args); err != nil {
		zap.L().Error("error running application", zap.Error(err))
		os.Exit(1)
	}
}

func app() *cli.App {
	return &cli.App{
		Name: "steel",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "log-level",
				Value: "INFO",
				Required: false,
			},
		},
		Before: func(ctx *cli.Context) error {
			return setupLogger(ctx)
		},
		Action: func(ctx *cli.Context) error {
			s, err := NewService(&ServiceConfig{
				// TODO: make all of this dynamic
				BackingStores: getBackingStoreConfiguration(),
				Partitioner:   MultiStorePartitionStrategy(2),
			})
			if err != nil {
				zap.L().Error("error creating service", zap.Error(err))
			}
			go s.Run()

			<-ctx.Done()
			return nil
		},
	}
}

func getBackingStoreConfiguration() []BackingStore {
	stores := make([]BackingStore, 0)
	stores = append(stores, BackingStore{
		Type:      store.StoreTypeRedis,
		Partition: 0,
		Address:   ":6379",
	})
	stores = append(stores, BackingStore{
		Type:      store.StoreTypeRedis,
		Partition: 1,
		Address:   ":6380",
	})
	return stores
}

func setupLogger(ctx *cli.Context) error {
	ecfg := zap.NewProductionEncoderConfig()
	ecfg.EncodeTime = zapcore.RFC3339TimeEncoder
	encoder := zapcore.NewConsoleEncoder(ecfg)
	level, err := zapcore.ParseLevel(ctx.String("log-level"))
	if err != nil {
		return err
	}
	core := zapcore.NewCore(encoder, os.Stdout, level)
	zap.ReplaceGlobals(zap.New(core))
	return nil
}
