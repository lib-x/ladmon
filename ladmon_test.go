package ladmon

import (
	"context"
	"fmt"
	"github.com/ory/ladon"
	"time"
)

func Example() {
	// Create manager
	manager, err := NewMongoManager(
		"mongodb://localhost:27017",
		"ladon",
		10*time.Second,
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
		ID:          "1",
		Description: "Test policy",
		Subjects:    []string{"user:1", "group:admin"},
		Resources:   []string{"resource:1"},
		Actions:     []string{"create", "read"},
		Effect:      ladon.AllowAccess,
		Conditions: ladon.Conditions{
			"ip": &ladon.CIDRCondition{
				CIDR: "192.168.1.0/24",
			},
		},
	}

	// Save policy
	if err := manager.Create(context.Background(), policy); err != nil {
		panic(err)
	}

	// Check permission
	request := &ladon.Request{
		Subject:  "user:1",
		Resource: "resource:1",
		Action:   "read",
	}

	if err := warden.IsAllowed(context.TODO(), request); err != nil {
		fmt.Println("Access denied:", err)
	} else {
		fmt.Println("Access allowed")
	}
}
