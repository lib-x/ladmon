package ladmon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ory/ladon"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoPolicy represents the policy model stored in MongoDB
type MongoPolicy struct {
	ID          string `bson:"id"`
	Description string `bson:"description"`
	Effect      string `bson:"effect"`
}

// MongoSubject represents the subject model stored in MongoDB
type MongoSubject struct {
	PolicyID string `bson:"policy_id"`
	Subject  string `bson:"subject"`
}

// MongoResource represents the resource model stored in MongoDB
type MongoResource struct {
	PolicyID string `bson:"policy_id"`
	Resource string `bson:"resource"`
}

// MongoAction represents the action model stored in MongoDB
type MongoAction struct {
	PolicyID string `bson:"policy_id"`
	Action   string `bson:"action"`
}

// MongoCondition represents the condition model stored in MongoDB
type MongoCondition struct {
	PolicyID string `bson:"policy_id"`
	Type     string `bson:"type"`
	Key      string `bson:"key"`
	Value    string `bson:"value"`
}

// MongoManager implements the ladon.Manager interface
type MongoManager struct {
	client      *mongo.Client
	database    string
	timeout     time.Duration
	collections struct {
		policies   string
		subjects   string
		resources  string
		actions    string
		conditions string
	}
}

func (m *MongoManager) FindPoliciesForSubject(ctx context.Context, subject string) (ladon.Policies, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	db := m.client.Database(m.database)

	// Find all subject documents matching the subject pattern
	cursor, err := db.Collection(m.collections.subjects).Find(ctx, bson.M{"subject": subject})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find matching subjects")
	}
	defer cursor.Close(ctx)

	// Collect matching policy IDs
	var policyIDs []string
	for cursor.Next(ctx) {
		var subject MongoSubject
		if err := cursor.Decode(&subject); err != nil {
			return nil, errors.Wrap(err, "failed to decode subject")
		}
		policyIDs = append(policyIDs, subject.PolicyID)
	}

	// Get detailed information for each policy
	var policies ladon.Policies
	for _, id := range policyIDs {
		policy, err := m.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ladon.ErrNotFound) {
				continue
			}
			return nil, errors.Wrap(err, fmt.Sprintf("failed to get policy %s", id))
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

func (m *MongoManager) FindPoliciesForResource(ctx context.Context, resource string) (ladon.Policies, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	db := m.client.Database(m.database)

	// Find all resource documents matching the resource pattern
	cursor, err := db.Collection(m.collections.resources).Find(ctx, bson.M{"resource": resource})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find matching resources")
	}
	defer cursor.Close(ctx)

	// Collect matching policy IDs
	var policyIDs []string
	for cursor.Next(ctx) {
		var resource MongoResource
		if err := cursor.Decode(&resource); err != nil {
			return nil, errors.Wrap(err, "failed to decode resource")
		}
		policyIDs = append(policyIDs, resource.PolicyID)
	}

	// Get detailed information for each policy
	var policies ladon.Policies
	for _, id := range policyIDs {
		policy, err := m.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ladon.ErrNotFound) {
				continue
			}
			return nil, errors.Wrap(err, fmt.Sprintf("failed to get policy %s", id))
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// NewMongoManager creates a new MongoManager
func NewMongoManager(ctx context.Context, uri, database string, timeout time.Duration) (*MongoManager, error) {
	connectOption := options.Client().ApplyURI(uri)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := mongo.Connect(ctx, connectOption)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create mongo client")
	}
	m := &MongoManager{
		client:   client,
		database: database,
		timeout:  timeout,
	}

	// Set collection names
	m.collections.policies = "ladon_policy"
	m.collections.subjects = "ladon_subject"
	m.collections.resources = "ladon_resource"
	m.collections.actions = "ladon_action"
	m.collections.conditions = "ladon_condition"

	// Create indexes
	err = m.setupIndexes(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup indexes")
	}

	return m, nil
}

// setupIndexes creates indexes for collections
func (m *MongoManager) setupIndexes(ctx context.Context) error {
	db := m.client.Database(m.database)

	// Subject collection index
	_, err := db.Collection(m.collections.subjects).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "policy_id", Value: 1},
			{Key: "subject", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	// Resource collection index
	_, err = db.Collection(m.collections.resources).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "policy_id", Value: 1},
			{Key: "resource", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	// Action collection index
	_, err = db.Collection(m.collections.actions).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "policy_id", Value: 1},
			{Key: "action", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	// Condition collection index
	_, err = db.Collection(m.collections.conditions).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "policy_id", Value: 1},
			{Key: "key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return err
	}

	return nil
}

// Create adds a policy to MongoDB .transaction not supported yet
func (m *MongoManager) Create(ctx context.Context, policy ladon.Policy) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	db := m.client.Database(m.database)

	// 1. Save the policy
	mongoPolicy := &MongoPolicy{
		ID:          policy.GetID(),
		Description: policy.GetDescription(),
		Effect:      policy.GetEffect(),
	}

	_, err := db.Collection(m.collections.policies).InsertOne(ctx, mongoPolicy)
	if err != nil {
		return errors.Wrap(err, "failed to create policy")
	}

	// 2. Save subjects
	for _, subject := range policy.GetSubjects() {
		_, err = db.Collection(m.collections.subjects).InsertOne(ctx, MongoSubject{
			PolicyID: policy.GetID(),
			Subject:  subject,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create subject")
		}
	}

	// 3. Save resources
	for _, resource := range policy.GetResources() {
		_, err = db.Collection(m.collections.resources).InsertOne(ctx, MongoResource{
			PolicyID: policy.GetID(),
			Resource: resource,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create resource")
		}
	}

	// 4. Save actions
	for _, action := range policy.GetActions() {
		_, err = db.Collection(m.collections.actions).InsertOne(ctx, MongoAction{
			PolicyID: policy.GetID(),
			Action:   action,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create action")
		}
	}

	// 5. Save conditions
	for key, condition := range policy.GetConditions() {
		// Serialize condition value
		value, err := json.Marshal(condition)
		if err != nil {
			return errors.Wrap(err, "failed to marshal condition")
		}

		_, err = db.Collection(m.collections.conditions).InsertOne(ctx, MongoCondition{
			PolicyID: policy.GetID(),
			Type:     fmt.Sprintf("%T", condition),
			Key:      key,
			Value:    string(value),
		})
		if err != nil {
			return errors.Wrap(err, "failed to create condition")
		}
	}
	return nil
}

// Update updates a policy in MongoDB
func (m *MongoManager) Update(ctx context.Context, policy ladon.Policy) error {
	// Delete the old policy and create a new one
	if err := m.Delete(ctx, policy.GetID()); err != nil {
		return err
	}
	return m.Create(ctx, policy)
}

// Get retrieves a policy from MongoDB
func (m *MongoManager) Get(ctx context.Context, id string) (ladon.Policy, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	db := m.client.Database(m.database)

	// 1. Get the policy
	var mongoPolicy MongoPolicy
	err := db.Collection(m.collections.policies).FindOne(ctx, bson.M{"id": id}).Decode(&mongoPolicy)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ladon.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to get policy")
	}

	// Create a default policy
	policy := &ladon.DefaultPolicy{
		ID:          mongoPolicy.ID,
		Description: mongoPolicy.Description,
		Effect:      mongoPolicy.Effect,
	}

	// 2. Get subjects
	cursor, err := db.Collection(m.collections.subjects).Find(ctx, bson.M{"policy_id": id})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find subjects")
	}
	defer cursor.Close(ctx)

	var subjects []string
	for cursor.Next(ctx) {
		var subject MongoSubject
		if err := cursor.Decode(&subject); err != nil {
			return nil, errors.Wrap(err, "failed to decode subject")
		}
		subjects = append(subjects, subject.Subject)
	}
	policy.Subjects = subjects

	// 3. Get resources
	cursor, err = db.Collection(m.collections.resources).Find(ctx, bson.M{"policy_id": id})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find resources")
	}
	defer cursor.Close(ctx)

	var resources []string
	for cursor.Next(ctx) {
		var resource MongoResource
		if err := cursor.Decode(&resource); err != nil {
			return nil, errors.Wrap(err, "failed to decode resource")
		}
		resources = append(resources, resource.Resource)
	}
	policy.Resources = resources

	// 4. Get actions
	cursor, err = db.Collection(m.collections.actions).Find(ctx, bson.M{"policy_id": id})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find actions")
	}
	defer cursor.Close(ctx)

	var actions []string
	for cursor.Next(ctx) {
		var action MongoAction
		if err := cursor.Decode(&action); err != nil {
			return nil, errors.Wrap(err, "failed to decode action")
		}
		actions = append(actions, action.Action)
	}
	policy.Actions = actions

	// 5. Get conditions
	cursor, err = db.Collection(m.collections.conditions).Find(ctx, bson.M{"policy_id": id})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find conditions")
	}
	defer cursor.Close(ctx)

	conditions := ladon.Conditions{}
	for cursor.Next(ctx) {
		var mongoCondition MongoCondition
		if err := cursor.Decode(&mongoCondition); err != nil {
			return nil, errors.Wrap(err, "failed to decode condition")
		}

		// Create condition of the correct type
		var condition ladon.Condition
		switch mongoCondition.Type {
		case "github.com/ory/ladon.StringEqualCondition", "*ladon.StringEqualCondition":
			condition = new(ladon.StringEqualCondition)
		case "github.com/ory/ladon.CIDRCondition", "*ladon.CIDRCondition":
			condition = new(ladon.CIDRCondition)
		case "github.com/ory/ladon.EqualsSubjectCondition", "*ladon.EqualsSubjectCondition":
			condition = new(ladon.EqualsSubjectCondition)
		case "github.com/ory/ladon.StringMatchCondition", "*ladon.StringMatchCondition":
			condition = new(ladon.StringMatchCondition)
		case "github.com/ory/ladon.StringPairsEqualCondition", "*ladon.StringPairsEqualCondition":
			condition = new(ladon.StringPairsEqualCondition)
		default:
			// Default to string equal condition
			condition = new(ladon.StringEqualCondition)
		}

		// Deserialize the condition
		if err := json.Unmarshal([]byte(mongoCondition.Value), condition); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal condition")
		}

		conditions[mongoCondition.Key] = condition
	}
	policy.Conditions = conditions

	return policy, nil
}

// Delete removes a policy from MongoDB
func (m *MongoManager) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	// Start a session for transaction
	session, err := m.client.StartSession()
	if err != nil {
		return errors.Wrap(err, "failed to start session")
	}
	defer session.EndSession(ctx)

	// Execute operations within a transaction
	callback := func(sessionContext mongo.SessionContext) (interface{}, error) {
		db := m.client.Database(m.database)

		// Delete data from all related collections
		_, err := db.Collection(m.collections.policies).DeleteOne(sessionContext, bson.M{"id": id})
		if err != nil {
			return nil, errors.Wrap(err, "failed to delete policy")
		}

		_, err = db.Collection(m.collections.subjects).DeleteMany(sessionContext, bson.M{"policy_id": id})
		if err != nil {
			return nil, errors.Wrap(err, "failed to delete subjects")
		}

		_, err = db.Collection(m.collections.resources).DeleteMany(sessionContext, bson.M{"policy_id": id})
		if err != nil {
			return nil, errors.Wrap(err, "failed to delete resources")
		}

		_, err = db.Collection(m.collections.actions).DeleteMany(sessionContext, bson.M{"policy_id": id})
		if err != nil {
			return nil, errors.Wrap(err, "failed to delete actions")
		}

		_, err = db.Collection(m.collections.conditions).DeleteMany(sessionContext, bson.M{"policy_id": id})
		if err != nil {
			return nil, errors.Wrap(err, "failed to delete conditions")
		}

		return nil, nil
	}

	_, err = session.WithTransaction(ctx, callback)
	return err
}

// GetAll retrieves all policies from MongoDB
func (m *MongoManager) GetAll(ctx context.Context, limit, offset int64) (policies ladon.Policies, err error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	db := m.client.Database(m.database)

	// Query all policy IDs
	findOptions := options.Find()
	if limit > 0 {
		findOptions.SetLimit(limit)
	}
	if offset > 0 {
		findOptions.SetSkip(offset)
	}

	cursor, err := db.Collection(m.collections.policies).Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find policies")
	}
	defer cursor.Close(ctx)

	var policyIDs []string
	for cursor.Next(ctx) {
		var policy MongoPolicy
		if err := cursor.Decode(&policy); err != nil {
			return nil, errors.Wrap(err, "failed to decode policy")
		}
		policyIDs = append(policyIDs, policy.ID)
	}

	// Get detailed information for each policy
	for _, id := range policyIDs {
		policy, err := m.Get(ctx, id)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("failed to get policy %s", id))
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// FindRequestCandidates finds all policies that could match the request
func (m *MongoManager) FindRequestCandidates(ctx context.Context, r *ladon.Request) (policies ladon.Policies, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	db := m.client.Database(m.database)

	// 1. Find policies that might match the subject
	subjectFilter := bson.M{"$or": []bson.M{
		{"subject": r.Subject},
		{"subject": ".*"},   // Any subject
		{"subject": "<.*>"}, // Regex pattern
	}}

	// Get matching policy IDs
	cursor, err := db.Collection(m.collections.subjects).Find(ctx, subjectFilter)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find matching subjects")
	}

	// Collect matching policy IDs
	var policyIDs []string
	for cursor.Next(ctx) {
		var subject MongoSubject
		if err := cursor.Decode(&subject); err != nil {
			return nil, errors.Wrap(err, "failed to decode subject")
		}
		policyIDs = append(policyIDs, subject.PolicyID)
	}
	cursor.Close(ctx)

	// If no matching policies, return empty result
	if len(policyIDs) == 0 {
		return []ladon.Policy{}, nil
	}

	// 2. Remove duplicate policy IDs
	uniquePolicyIDs := make(map[string]bool)
	for _, id := range policyIDs {
		uniquePolicyIDs[id] = true
	}

	// 3. Get detailed information for each policy
	for id := range uniquePolicyIDs {
		policy, err := m.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ladon.ErrNotFound) {
				continue
			}
			return nil, errors.Wrap(err, fmt.Sprintf("failed to get policy %s", id))
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// Close closes the MongoDB connection
func (m *MongoManager) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	return m.client.Disconnect(ctx)
}
