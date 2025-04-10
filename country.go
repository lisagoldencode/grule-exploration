package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

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

type CountryMusicDocument struct {
	RuleID     string
	Artist     string
	Title      string
	LyricQuote string
	VideoLink  string
	Themes     map[string]string
}

type UserSelections struct {
	Adventure          bool
	America            bool
	CarsTrucksTractors bool
	Goodtimes          bool
	Grit               bool
	Home               bool
	Love               bool
	HeartBreak         bool
	Lessons            bool
	Rebellion          bool
	Recommendations    map[string]int
}

type IncomingRequest struct {
	Themes map[string]bool `json:"themes"`
}

func (p *UserSelections) GetField(fieldName string) (bool, error) {
	val := reflect.ValueOf(p).Elem()
	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return false, fmt.Errorf("field '%s' does not exist", fieldName)
	}

	if field.Kind() == reflect.Bool {
		return field.Bool(), nil
	}

	return false, fmt.Errorf("field '%s' is not a boolean", fieldName)
}

func (p *UserSelections) IsSongThemeMatch(songId string, songThemes ...string) bool {
	fmt.Println("=========")
	fmt.Println("Checking Matches: " + songId)

	for _, theme := range songThemes {
		boolValue, err := p.GetField(theme)

		if err != nil {
			panic(err)
		}
		if boolValue {
			fmt.Println("Match found!")
			return true
		}

		fmt.Println("No Matches")
	}
	return false
}

func (p *UserSelections) SetRecommendations(songId string, songThemes ...string) int {
	fmt.Println("------")
	fmt.Println("Counting Matches... (" + songId + ")")

	matchCount := 0
	for _, theme := range songThemes {
		boolValue, err := p.GetField(theme)

		if err != nil {
			panic(err)
		}
		if err != nil {
			panic(err)
		}

		if boolValue {
			matchCount += 1
			fmt.Println(theme+" --- Match found -", matchCount)
		} else {
			fmt.Println(theme)
		}
	}

	fmt.Println("\nMatches for song '"+songId+"':", matchCount)

	p.Recommendations[songId] = matchCount
	return matchCount
}

func handleRequest(ctx context.Context, event json.RawMessage) (json.RawMessage, error) {

	userSelections := getUserSelections(event)

	fmt.Printf("Parsed UserSelections: %+v\n", userSelections)

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

	recommendations := extractRecommendations(resp.Items)

	//Generate Grule rules based on what is present int he recommendations array
	documentRules := extractGrules(recommendations)

	//Rule Definition
	drls := `
    rule Check10000 "Take Me Home, Country Roads" salience 10 {
        when
          UserSelections.IsSongThemeMatch("10000", "Lessons", "Adventure", "Home", "America")
        then
        	UserSelections.SetRecommendations("10000", "Lessons", "Adventure", "Home", "America");
            Retract("Check10000");
    }

	rule Check10001 "All My Ex's Live In Texas" salience 10 {
        when
           UserSelections.IsSongThemeMatch("10001", "Rebellion", "HeartBreak", "America")
        then
            UserSelections.SetRecommendations("10001", "Rebellion", "HeartBreak", "America");
            Retract("Check10001");
    }
    `

	fmt.Println("DynamoDb Rules: ")
	fmt.Println(documentRules) // Print the combined rule set

	fmt.Println("Hardcoded Rules: ")
	fmt.Println(drls)

	//Get GRULE working
	dataCtx := ast.NewDataContext()
	dataCtx.Add("UserSelections", userSelections)

	knowledgeLibrary := ast.NewKnowledgeLibrary()
	ruleBuilder := builder.NewRuleBuilder(knowledgeLibrary)

	bs := pkg.NewBytesResource([]byte(documentRules))
	err = ruleBuilder.BuildRuleFromResource("SongRecs", "0.0.1", bs)
	if err != nil {
		panic(err)
	}

	knowledgeBase, err := knowledgeLibrary.NewKnowledgeBaseInstance("SongRecs", "0.0.1")

	engine := engine.NewGruleEngine()
	err = engine.Execute(dataCtx, knowledgeBase)
	if err != nil {
		panic(err)
	}

	fmt.Println("\n==========")
	fmt.Println("Song recommendations for user: ")
	fmt.Println(userSelections.Recommendations)

	//return "Success", nil
	responseData, err := json.Marshal(recommendations)
	return responseData, nil // Convert []byte to string
}

func getUserSelections(event json.RawMessage) *UserSelections {
	var incoming IncomingRequest
	if err := json.Unmarshal([]byte(event), &incoming); err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
	}

	// Map the JSON fields to UserSelections struct
	userSelections := UserSelections{
		Adventure:          incoming.Themes["adventure"],
		America:            incoming.Themes["america"],
		CarsTrucksTractors: incoming.Themes["carsTrucksTractors"],
		Goodtimes:          incoming.Themes["goodtimes"],
		Grit:               incoming.Themes["grit"],
		Home:               incoming.Themes["home"],
		Love:               incoming.Themes["love"],
		HeartBreak:         incoming.Themes["heartbreak"],
		Lessons:            incoming.Themes["lessons"],
		Rebellion:          incoming.Themes["rebellion"],
		Recommendations:    make(map[string]int), // Initialize Recommendations
	}
	return &userSelections
}

func extractGrules(documents []CountryMusicDocument) string {
	var rules []string

	for _, document := range documents {
		ruleFormat := `rule Check%s "%s" salience 10 {
            when
               UserSelections.IsSongThemeMatch(%s, %s)
            then
               UserSelections.SetRecommendations(%s, %s);
               Retract("Check%s");
        }`
		themes := []string{}
		for theme, desc := range document.Themes {
			if desc != "" {
				themes = append(themes, fmt.Sprintf("\"%s\"", capitalizeFirstLetter(theme)))
			}
		}

		rules = append(rules, fmt.Sprintf(ruleFormat, document.RuleID, document.Title, // Rule function
			fmt.Sprintf("\"%s\"", document.RuleID), strings.Join(themes, ", "), // When
			fmt.Sprintf("\"%s\"", document.RuleID), strings.Join(themes, ", "), // Then
			document.RuleID)) // Retract
	}

	songRule := strings.Join(rules, "\n\n") // Combine all rules into one string
	return songRule
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

func capitalizeFirstLetter(s string) string {
	if len(s) == 0 {
		return s // Return empty string if input is empty
	}
	return strings.ToUpper(s[:1]) + s[1:] // Capitalize first letter and append the rest
}
