package ladmon

import (
	"context"
	"fmt"
	"github.com/ory/ladon"
	"testing"
	"time"
)

func Example() {
	// Create manager
	manager, err := NewMongoManager(
		"mongodb://localhost:27017",
		"ladon",
		20*time.Second,
	)
	if err != nil {
		panic(err)
	}
	defer manager.Close()

	// Use with Ladon
	warden := &ladon.Ladon{
		Manager: manager,
	}

	// Create policy
	policy := &ladon.DefaultPolicy{
		ID:          "3",
		Description: "Test policy 3",
		Subjects:    []string{"user:3", "group:admin"},
		Resources:   []string{"resource:3"},
		Actions:     []string{"create", "read"},
		Effect:      ladon.AllowAccess,
	}

	// Save policy
	if err := manager.Create(context.Background(), policy); err != nil {
		panic(err)
	}

	// Check permission
	request := &ladon.Request{
		Subject:  "user:3",
		Resource: "resource:3",
		Action:   "read",
	}

	if err := warden.IsAllowed(context.TODO(), request); err != nil {
		fmt.Println("Access denied:", err)
	} else {
		fmt.Println("Access allowed")
	}
}

func Test_Example(t *testing.T) {
	Example()
}
