package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
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
		if boolValue {
			matchCount += 1
			fmt.Println(theme+" --- Match found -", matchCount)
		} else {
			fmt.Println(theme)
		}
	}

	matchCount = matchCount * 10

	//Slightly penalize themes unselected
	matchCount = matchCount - (len(songThemes) - matchCount)

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
	//Generate Grule rules based on what is present int he recommendations array

	documents := extractJSONFromDocuments(resp.Items)
	documentRules := extractGrules(documents)

	fmt.Println("DynamoDb Rules: ")
	fmt.Println(documentRules) // Print the combined rule set

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

	//return "Success", nil
	userRecs := filterDocumentsByRecommendations(documents, userSelections)
	responseData, err := json.Marshal(userRecs)
	return responseData, nil // Convert []byte to string
}

func filterDocumentsByRecommendations(documents []CountryMusicDocument, userSelections *UserSelections) []CountryMusicDocument {
	fmt.Println("Starting filterDocumentsByRecommendations...")

	// Print the user preferences
	fmt.Println("Method input: User Preferences:")
	fmt.Printf("Adventure: %t, America: %t, CarsTrucksTractors: %t, Goodtimes: %t, Grit: %t, Home: %t, Love: %t, HeartBreak: %t, Lessons: %t, Rebellion: %t\n",
		userSelections.Adventure, userSelections.America, userSelections.CarsTrucksTractors, userSelections.Goodtimes, userSelections.Grit,
		userSelections.Home, userSelections.Love, userSelections.HeartBreak, userSelections.Lessons, userSelections.Rebellion)
	fmt.Printf("Method input: Recommendations: %v\n", userSelections.Recommendations)

	// Get top N recommendations
	fmt.Println("Retrieving top recommended RuleIDs...")
	topRuleIDs := getTopNRecommendations(userSelections.Recommendations, 3)
	fmt.Printf("Top RuleIDs: %v\n", topRuleIDs)

	// Filter documents based on RuleID
	fmt.Println("Filtering documents based on top RuleIDs...")
	filteredDocs := filterDocuments(documents, topRuleIDs)
	fmt.Printf("Filtered Documents Count: %d\n", len(filteredDocs))

	// Generate new list with updated themes based on UserSelections
	fmt.Println("Updating themes in filtered documents based on user selections...")
	themeUpdatedFilteredDocs := generateThemeUpdatedDocs(filteredDocs, *userSelections)
	fmt.Printf("Theme-Updated Documents Count: %d\n", len(themeUpdatedFilteredDocs))

	fmt.Println("Final filtered and updated documents:")
	for _, doc := range themeUpdatedFilteredDocs {
		fmt.Printf("RuleID: %s, Artist: %s, Title: %s, Themes: %v\n", doc.RuleID, doc.Artist, doc.Title, doc.Themes)
	}

	fmt.Println("Completed filterDocumentsByRecommendations.")
	return themeUpdatedFilteredDocs
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

func extractJSONFromDocuments(items []map[string]types.AttributeValue) []CountryMusicDocument {
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

// Function to get the top N recommendations
func getTopNRecommendations(recommendations map[string]int, N int) []string {
	var sortedList []struct {
		Key   string
		Value int
	}

	for k, v := range recommendations {
		sortedList = append(sortedList, struct {
			Key   string
			Value int
		}{k, v})
	}

	sort.Slice(sortedList, func(i, j int) bool {
		return sortedList[i].Value > sortedList[j].Value
	})

	if len(sortedList) < N {
		N = len(sortedList)
	}

	var topRuleIDs []string
	for _, item := range sortedList[:N] {
		topRuleIDs = append(topRuleIDs, item.Key)
	}

	return topRuleIDs
}

// Function to filter documents based on matching RuleID
func filterDocuments(documents []CountryMusicDocument, topRuleIDs []string) []CountryMusicDocument {
	var filteredDocuments []CountryMusicDocument
	ruleIDMap := make(map[string]bool)

	for _, id := range topRuleIDs {
		ruleIDMap[id] = true
	}

	for _, doc := range documents {
		if ruleIDMap[doc.RuleID] {
			filteredDocuments = append(filteredDocuments, doc)
		}
	}

	return filteredDocuments
}

// Function to create a new list with updated themes based on UserSelections
func generateThemeUpdatedDocs(filteredDocs []CountryMusicDocument, userSelections UserSelections) []CountryMusicDocument {
	// Normalize theme keys to lowercase for consistent lookup
	themeKeys := map[string]bool{
		strings.ToLower("Adventure"):          userSelections.Adventure,
		strings.ToLower("America"):            userSelections.America,
		strings.ToLower("CarsTrucksTractors"): userSelections.CarsTrucksTractors,
		strings.ToLower("Goodtimes"):          userSelections.Goodtimes,
		strings.ToLower("Grit"):               userSelections.Grit,
		strings.ToLower("Home"):               userSelections.Home,
		strings.ToLower("Love"):               userSelections.Love,
		strings.ToLower("HeartBreak"):         userSelections.HeartBreak,
		strings.ToLower("Lessons"):            userSelections.Lessons,
		strings.ToLower("Rebellion"):          userSelections.Rebellion,
	}

	// Print the normalized themeKeys map to verify values
	fmt.Printf("themeKeys (normalized to lowercase): %v\n", themeKeys)

	var themeUpdatedFilteredDocs []CountryMusicDocument

	for _, doc := range filteredDocs {
		updatedThemes := make(map[string]string)
		for theme, value := range doc.Themes {
			normalizedTheme := strings.ToLower(theme) // Convert the document's theme to lowercase

			if themeKeys[normalizedTheme] {
				updatedThemes[theme] = value
			} else {
				updatedThemes[theme] = ""
			}
		}
		themeUpdatedFilteredDocs = append(themeUpdatedFilteredDocs, CountryMusicDocument{
			RuleID:     doc.RuleID,
			Artist:     doc.Artist,
			Title:      doc.Title,
			LyricQuote: doc.LyricQuote,
			VideoLink:  doc.VideoLink,
			Themes:     updatedThemes,
		})
	}

	return themeUpdatedFilteredDocs
}
