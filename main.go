package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/bwmarrin/discordgo"
)

var (
	config    DownloaderConfig
	outFile   *os.File
	csvWriter *csv.Writer
)

type DownloaderConfig struct {
	Token              string   `json:"token"`                       // the token that the client will use
	TargetGuildID      string   `json:"guildID"`                     // the target guild id to download from
	ExcludeChannelIDs  []string `json:"excludeChannelIDs,omitempty"` // channels to exclude when downloading
	FilePath           string   `json:"outputPath"`                  // where to save the output
	ConcurrentChannels int      `json:"concurrentChannels"`          // how many channels to process at once
	OmitEmpty          bool     `json:"omitEmpty,"`                  // whether to leave out empty messages (skews message totals)
}

func main() {
	configBytes, err := os.ReadFile("config.json")

	if err != nil {
		fmt.Println(err)
		return
	}

	err = json.Unmarshal(configBytes, &config)

	if err != nil {
		fmt.Println(err)
		return
	} else if len(config.Token) == 0 {
		fmt.Println("Config token is empty! Please provide a token!")
		return
	}

	discord, err := discordgo.New(fmt.Sprintf("Bot %s", config.Token))

	if err != nil {
		fmt.Println(err)
		return
	}

	guild, err := discord.Guild(config.TargetGuildID)

	if err != nil {
		fmt.Println(err)
		return
	}

	channels, err := discord.GuildChannels(config.TargetGuildID)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Targeting guild: %s (%d channels)\n", guild.Name, len(channels))

	var textChannels []*discordgo.Channel
Outer:
	for _, channel := range channels {
		for _, excl := range config.ExcludeChannelIDs {
			if excl == channel.ID {
				goto Outer
			}
		}
		switch channel.Type {
		case discordgo.ChannelTypeGuildText:
			textChannels = append(textChannels, channel)
		case discordgo.ChannelTypeGuildNews:
			textChannels = append(textChannels, channel)
		}
	}

	textChannelChannel := make(chan *discordgo.Channel, len(textChannels))
	for _, channel := range textChannels {
		textChannelChannel <- channel
	}
	close(textChannelChannel)

	fmt.Printf("%d text channels\n", len(textChannelChannel))

	outFile, err = os.Create(config.FilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer outFile.Close()

	csvWriter = csv.NewWriter(outFile)
	csvWriter.Comma = '\u001E'

	writeToCSV([]string{"author", "textContent", "id"})

	var wg sync.WaitGroup
	wg.Add(config.ConcurrentChannels)

	for i := 0; i < config.ConcurrentChannels; i++ {
		go channelMessagesWorker(discord, &wg, textChannelChannel)
	}

	wg.Wait()
}

func writeToCSV(data []string) {
	if err := csvWriter.Write(data); err != nil {
		fmt.Printf("error writing data to csv: %v\n", err)
		fmt.Printf("%v\n", data)
	}
	csvWriter.Flush()
}

func channelMessagesWorker(client *discordgo.Session, waitGroup *sync.WaitGroup, textChannels chan *discordgo.Channel) {
	defer waitGroup.Done()

	for channel := range textChannels {
		var latest []*discordgo.Message
		beforeID := ""
		total := 0

		fmt.Printf("Starting download for channel %s (%s)\n", channel.Name, channel.ID)
		for len(latest) == 100 || len(beforeID) == 0 {
			var err error
			latest, err = client.ChannelMessages(channel.ID, 100, beforeID, "", "")

			if err != nil {
				fmt.Printf("Error while downloading channel %s (%s): %v\n", channel.Name, channel.ID, err)
			}

			for _, message := range latest {
				if config.OmitEmpty && len(message.Content) == 0 {
					continue
				}
				writeToCSV([]string{message.Author.ID, message.Content, message.ID})
			}

			if len(latest) < 1 {
				beforeID = "a"
				continue
			}
			beforeID = latest[len(latest)-1].ID
			total += len(latest)

			// fmt.Printf("Downloaded %d messages (total: %d) from %s (%s) Last message ID: %s\n", len(latest), total, channel.Name, channel.ID, beforeID)
		}
		fmt.Printf("Downloaded %d messages from %s (%s)\n", total, channel.Name, channel.ID)
	}
}
