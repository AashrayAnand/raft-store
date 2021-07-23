package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type BankApp struct {
	kvs    *rsm       // raft replicated state machine
	server *fiber.App // http server
}

// Create a new account with a given initial balance. Equivalent
// of key-value set
func (app *BankApp) CreateAccount(ctx *fiber.Ctx) error {
	name := ctx.Params("name")
	balance, err := strconv.ParseFloat(ctx.Params("balance"), 32)
	if err != nil {
		log := fmt.Sprintf("Failed to parse balance %s, please provide a valid numerical balance", ctx.Params("balance"))
		return ctx.SendString(log)
	} else {
		app.kvs.ProposeCreate(name, float32(balance))
		log := fmt.Sprintf("Creating account with name %s, and initial balance %f", name, balance)
		return ctx.SendString(log)
	}
}

// Delete an new account with a given name
func (app *BankApp) DeleteAccount(ctx *fiber.Ctx) error {
	name := ctx.Params("name")
	app.kvs.ProposeDelete(name)
	log := fmt.Sprintf("Deleting account %s", name)
	return ctx.SendString(log)
}

func (app *BankApp) GetBalance(ctx *fiber.Ctx) error {
	name := ctx.Params("name")
	value, ok := app.kvs.Lookup(name)
	var log string
	if !ok {
		log = fmt.Sprintf("Account %s does not exist", name)
	} else {
		log = fmt.Sprintf("Account %s has balance %f", name, value)
	}
	return ctx.SendString(log)
}

func (app *BankApp) ListAccounts(ctx *fiber.Ctx) error {
	accounts := app.kvs.List()
	log := fmt.Sprintf("List of accounts: %v", accounts)
	return ctx.SendString(log)
}

func (app *BankApp) TransferMoney(ctx *fiber.Ctx) error {
	from := ctx.Params("from")
	to := ctx.Params("to")
	amount, err := strconv.ParseFloat(ctx.Params("amount"), 32)
	if err != nil {
		log := fmt.Sprintf("Failed to parse amount %s, please provide a valid numerical amount", ctx.Params("amount"))
		return ctx.SendString(log)
	} else {
		app.kvs.ProposeTransfer(from, to, float32(amount))
		log := fmt.Sprintf("Transferring %f from %s to %s", amount, from, to)
		return ctx.SendString(log)
	}
}

func (app *BankApp) AddRoutes() {
	app.server.Get("/", (func(ctx *fiber.Ctx) error { return ctx.SendString("Hello World!") }))
	// /Create/john/100 -> new bank account for john with initial balance of 100
	app.server.Post("/Create/:name/:balance", app.CreateAccount)

	// /Delete/John
	app.server.Post("/Delete/:name", app.DeleteAccount)

	// /Balance/john -> current balance of john
	app.server.Get("/Balance/:name", app.GetBalance)

	// /List/ -> list all accounts
	app.server.Get("/List", app.ListAccounts)

	// /Transfer/john/alice/50 -> transfer 50 from john to alice
	app.server.Put("/Transfer/:from/:to/:amount", app.TransferMoney)
}

func NewBankApp(kvs *rsm, cluster *string, nodeId *int, port *int) BankApp {
	app := BankApp{kvs: kvs, server: fiber.New()}
	app.AddRoutes()
	return app
}

func ServeApp(kvs *rsm, cluster *string, nodeId *int, port *int) {
	app := NewBankApp(kvs, cluster, nodeId, port)
	log.Fatal(app.server.Listen(fmt.Sprintf(":%s", strconv.Itoa(*port))))
}
