package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hyperjumptech/grule-rule-engine/ast"
	"github.com/hyperjumptech/grule-rule-engine/builder"
	"github.com/hyperjumptech/grule-rule-engine/engine"
	"github.com/hyperjumptech/grule-rule-engine/pkg"
)

func main() {
	lambda.Start(handleRequest)
}

type MyFact struct {
	IntAttribute     int64
	StringAttribute  string
	BooleanAttribute bool
	FloatAttribute   float64
	TimeAttribute    time.Time
	WhatToSay        string
}

type CountryMusicDocument struct {
	RuleID     string
	Artist     string
	Title      string
	LyricQuote string
	VideoLink  string
	Themes     map[string]string
}

func handleRequest(ctx context.Context, event json.RawMessage) (json.RawMessage, error) {

	//Print hello
	log.Printf("Welcome to Lisa Golden's Tutorial App")

	//Call DynamoDB
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-2"),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)

	if err != nil {
		log.Fatalf("failed to list tables, %v", err)
	}

	// Specify the table name
	tableName := "CountryMusicRepo"

	resp, err := svc.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		log.Fatalf("Failed to scan items: %v", err)
	}

	fmt.Println("Items in Table:")

	for _, item := range resp.Items {
		printItem(item, "")
		fmt.Println("--------")
	}

	recommendations := extractRecommendations(resp.Items)

	//Generate Grule rules based on what is present int he recommendations array
	documentRules := extractGrules(recommendations)
	fmt.Println(documentRules) // Print the combined rule set

	//Get GRULE working
	myFact := &MyFact{
		IntAttribute:     123,
		StringAttribute:  "Some string value",
		BooleanAttribute: true,
		FloatAttribute:   1.234,
		TimeAttribute:    time.Now(),
	}

	dataCtx := ast.NewDataContext()
	err = dataCtx.Add("Tutorial", myFact)
	if err != nil {
		panic(err)
	}

	knowledgeLibrary := ast.NewKnowledgeLibrary()
	ruleBuilder := builder.NewRuleBuilder(knowledgeLibrary)

	//Rule Definition
	drls := `
    rule CheckValues "Check the default values" salience 10 {
        when
            Tutorial.IntAttribute == 123 && Tutorial.StringAttribute == "Some string value"
        then
            Tutorial.WhatToSay = Tutorial.GetWhatToSay("Hello Grule");
            Retract("CheckValues");
    }
    `
	bs := pkg.NewBytesResource([]byte(drls))
	err = ruleBuilder.BuildRuleFromResource("TutorialRules", "0.0.1", bs)
	if err != nil {
		panic(err)
	}

	knowledgeBase, err := knowledgeLibrary.NewKnowledgeBaseInstance("TutorialRules", "0.0.1")

	engine := engine.NewGruleEngine()
	err = engine.Execute(dataCtx, knowledgeBase)
	if err != nil {
		panic(err)
	}

	fmt.Println(myFact.WhatToSay)

	//return "Success", nil
	responseData, err := json.Marshal(recommendations)
	return responseData, nil // Convert []byte to string
}

func extractGrules(documents []CountryMusicDocument) string {
	var rules []string

	for _, document := range documents {
		ruleFormat := `rule Check%s "%s" salience 10 {
            when
               UserSelections.IsSongThemeMatch(%s)
            then
                UserSelections.Append("%s");
                Retract("Check%s");
        }`
		themes := []string{}
		for theme, desc := range document.Themes {
			if desc != "" {
				themes = append(themes, fmt.Sprintf("\"%s\"", theme))
			}
		}

		rules = append(rules, fmt.Sprintf(ruleFormat, document.RuleID, document.Title, strings.Join(themes, ", "), document.RuleID, document.RuleID))
	}

	songRule := strings.Join(rules, "\n\n") // Combine all rules into one string
	return songRule
}

func (my *MyFact) GetWhatToSay(sentance string) string {
	return fmt.Sprintf("Lets say \"%s\"", sentance)
}

// Function to recursively print item values
func printItem(item map[string]types.AttributeValue, indent string) {
	for key, value := range item {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			// String value
			fmt.Printf("%s%s: %s\n", indent, key, v.Value)
		case *types.AttributeValueMemberN:
			// Number value
			fmt.Printf("%s%s: %s\n", indent, key, v.Value)
		case *types.AttributeValueMemberM:
			// Map value, recursively print nested values
			fmt.Printf("%s%s:\n", indent, key)
			printItem(v.Value, indent+"  ") // Indentation for nested keys
		case *types.AttributeValueMemberL:
			// List value, iterate through list items
			fmt.Printf("%s%s: [\n", indent, key)
			for _, listItem := range v.Value {
				if mapItem, ok := listItem.(*types.AttributeValueMemberM); ok {
					printItem(mapItem.Value, indent+"  ")
				} else {
					fmt.Printf("%s  %v\n", indent, listItem)
				}
			}
			fmt.Printf("%s]\n", indent)
		default:
			fmt.Printf("%s%s: [unsupported type]\n", indent, key)
		}
	}
}

func extractRecommendations(items []map[string]types.AttributeValue) []CountryMusicDocument {
	var recommendations []CountryMusicDocument

	for _, item := range items {
		recommendation := CountryMusicDocument{
			RuleID:     getStringValue(item["RuleID"]),
			Artist:     getStringValue(item["artist"]),
			Title:      getStringValue(item["title"]),
			LyricQuote: getStringValue(item["lyricQuote"]),
			VideoLink:  getStringValue(item["videoLink"]),
			Themes:     extractThemes(item["themes"]),
		}

		recommendations = append(recommendations, recommendation)
	}

	return recommendations
}

// Helper function to extract a string value from DynamoDB attributes
func getStringValue(attr types.AttributeValue) string {
	if sAttr, ok := attr.(*types.AttributeValueMemberS); ok {
		return sAttr.Value
	}
	return ""
}

// Helper function to extract a map of themes
func extractThemes(attr types.AttributeValue) map[string]string {
	themes := make(map[string]string)
	if mAttr, ok := attr.(*types.AttributeValueMemberM); ok {
		for key, value := range mAttr.Value {
			themes[key] = getStringValue(value)
		}
	}
	return themes
}
