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
	wmu       sync.Mutex
	lmu       sync.Mutex

	loggerLines []string
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

	outFile, err = os.Create(config.FilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer outFile.Close()

	csvWriter = csv.NewWriter(outFile)
	csvWriter.Comma = '\u001E'

	writeToCSV([]string{"author", "textContent", "id"})

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

	var channels []*discordgo.Channel
	channels, err = discord.GuildChannels(config.TargetGuildID)

	if err != nil {
		fmt.Println(err)
		return
	}

	logger(fmt.Sprintf("Targeting guild: %s (%d channels)", guild.Name, len(channels)), -1)

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

	logger(fmt.Sprintf("%d text channels", len(textChannelChannel)), -1)

	var wg sync.WaitGroup
	wg.Add(config.ConcurrentChannels)

	for i := 0; i < config.ConcurrentChannels; i++ {
		go channelMessagesWorker(discord, &wg, textChannelChannel, i)
	}

	wg.Wait()
}

func writeToCSV(data []string) {
	wmu.Lock()
	defer wmu.Unlock()
	if err := csvWriter.Write(data); err != nil {
		panic(fmt.Sprintf("error writing data to csv: %v\n%v\n", err, data))
	}
	csvWriter.Flush()
}

func channelMessagesWorker(client *discordgo.Session, waitGroup *sync.WaitGroup, textChannels chan *discordgo.Channel, id int) {
	defer waitGroup.Done()

	for channel := range textChannels {
		var latest []*discordgo.Message
		beforeID := ""
		total := 0

		logger(fmt.Sprintf("[Worker %d] Starting download for channel %s (%s)", id, channel.Name, channel.ID), -1)
		for len(latest) == 100 || len(beforeID) == 0 {
			var err error
			latest, err = client.ChannelMessages(channel.ID, 100, beforeID, "", "")

			if err != nil {
				logger(fmt.Sprintf("[Worker %d] Error while downloading channel %s (%s): %v", id, channel.Name, channel.ID, err), -1)
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

			logger(fmt.Sprintf("> [Worker %d] Downloaded %d messages (total: %d) from %s (%s) Last message ID: %s", id, len(latest), total, channel.Name, channel.ID, beforeID), id)
		}
		logger(fmt.Sprintf("[Worker %d] Downloaded %d messages from %s (%s)", id, total, channel.Name, channel.ID), -1)
	}
}

func logger(line string, id int) {
	lmu.Lock()
	defer lmu.Unlock()
	if len(loggerLines) != config.ConcurrentChannels {
		loggerLines = make([]string, config.ConcurrentChannels)
		for i := range loggerLines {
			loggerLines[i] = "<empty string>"
		}
	}

	for i := 0; i < config.ConcurrentChannels-1; i++ {
		fmt.Print("\033[A")
	}
	if id < 0 || id >= config.ConcurrentChannels {
		fmt.Printf("\033[2K\r%s\n", line)
	} else {
		loggerLines[id] = line
	}
	for i := 0; i < config.ConcurrentChannels-1; i++ {
		fmt.Printf("\033[2K\r%s\n", loggerLines[i])
	}
	fmt.Printf("\033[2K\r%s", loggerLines[len(loggerLines)-1])
}
