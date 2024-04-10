#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include "nlohmann/json.hpp"

using json = nlohmann::json;

struct DataTime {
    std::string timeKey;
    double sentiment_score;
    int posts_num;

    DataTime(std::string key, double score, int num)
            : timeKey(std::move(key)), sentiment_score(score), posts_num(num) {}
};

struct TweetData {
    std::string created_at;
    double sentiment;
    bool setSentiment; // 新增一个标志来表示是否已经设置了sentiment

    TweetData() : sentiment(0.0), setSentiment(false) {} // 构造函数中初始化
};

void process_json(nlohmann::json& data, int rank, int size, std::vector<DataTime>& data_hour, std::vector<DataTime>& data_day) {
    std::map<std::string, double> sentiment_sum_by_hour;
    std::map<std::string, double> sentiment_sum_by_day;
    std::map<std::string, int> posts_sum_by_hour;
    std::map<std::string, int> posts_sum_by_day;

    // 假设data是按日期键控的JSON对象，每个键对应该日期的推文信息数组
    for (auto& date_entry : data.items()) {
        std::string date = date_entry.key(); // 日期
        auto tweets_of_date = date_entry.value(); // 这一天的所有推文

        for (auto &tweet: tweets_of_date) {
            std::string created_at = tweet["created_at"];
            double sentiment = tweet["sentiment"];
            std::tm tm = {};
            std::istringstream ss(created_at);
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");

            if (ss.fail()) {
                //std::cerr << "Failed to parse the created_at time: " << sentiment<< std::endl;
                continue; // 转到下一条推文
            }

            std::ostringstream os;
            os << std::put_time(&tm, "%Y-%m-%d-%H");
            std::string hour_key = os.str(); // 生成每小时的键

            os.str(""); // 清空ostringstream
            os << std::put_time(&tm, "%Y-%m-%d");
            std::string day_key = os.str(); // 生成每天的键

            sentiment_sum_by_hour[hour_key] += sentiment;
            sentiment_sum_by_day[day_key] += sentiment;
            posts_sum_by_hour[hour_key]++;
            posts_sum_by_day[day_key]++;
        }
    }
    //push hour data to vector
    for (const auto &pair: sentiment_sum_by_hour) {
        data_hour.emplace_back(pair.first, pair.second, posts_sum_by_hour[pair.first]);
    }

    //push day data to vector
    for (const auto &pair: sentiment_sum_by_day) {
        data_day.emplace_back(pair.first, pair.second, posts_sum_by_day[pair.first]);
    }
}

class CustomSAXHandler : public json::json_sax_t {
private:

    std::vector<TweetData> tweets;
    TweetData currentTweet;

    bool inDoc = false;
    bool inData = false;
    bool isSentimentFound = false;

    std::string currentKey; // Track the current key to identify when we're processing specific fields

    size_t count = 0; // 添加计数器
    const size_t max_tweets = 50000; // 最大处理的推文数

    // Your existing data structures for accumulating results
    std::map<std::string, double> sentiment_sum_by_hour;
    std::map<std::string, double> sentiment_sum_by_day;
    std::map<std::string, int> posts_sum_by_hour;
    std::map<std::string, int> posts_sum_by_day;

    // Other necessary state tracking variables

public:
    bool start_object(std::size_t elements) override {
        if (currentKey == "doc") {
            inDoc = true;
        } else if (inDoc && currentKey == "data") {
            inData = true;
        }
        return true;
    }


    bool end_object() override {
        if (inData) {
            // We're exiting a data object.
            if (!isSentimentFound) {
                // If sentiment was not found, set a default value.
                currentTweet.sentiment = 0.0;
            }
            // Now that we've finished processing the data object, add it to the list.
            tweets.push_back(currentTweet);
            currentTweet = TweetData(); // Reset for the next data object.
            inData = false; // No longer in a data object.
            isSentimentFound = false; // Reset for the next data object.
        } else if (inDoc) {
            // Exiting a doc object. If necessary, additional cleanup can go here.
            inDoc = false;
        }
        return true;
    }

    bool key(string_t& val) override {
        if (val == "data") {
            inData = true; // 开始解析data对象
            currentTweet = TweetData(); // 重置当前推文数据为一个新的TweetData对象
            isSentimentFound = false; // 重置sentiment发现状态
        } else if (inData) {
            // 在data对象内部，检查是否是我们感兴趣的字段
            if (val == "sentiment") {
                isSentimentFound = true; // 标记找到sentiment字段
            }
        }
        return true;
    }

    bool string(string_t& val) override {
        if (inData) {
            // 在data对象内，只处理created_at字段
            currentTweet.created_at = val;
        }
        return true;
    }

    bool number_integer(number_integer_t val) override {
        if (inData && isSentimentFound) {
            // 只有当我们在data对象内且之前找到了sentiment字段时处理
            currentTweet.sentiment = val;
        }
        return true;
    }

    bool number_float(number_float_t val, const string_t&) override {
        if (inData && isSentimentFound) {
            // 只有当我们在data对象内且之前找到了sentiment字段时处理
            currentTweet.sentiment = val;
        }
        return true;
    }

    bool start_array(std::size_t elements) override {
        // Implement based on your JSON structure
        return true;
    }

    bool end_array() override {
        // Implement based on your JSON structure
        return true;
    }

    bool boolean(bool val) override {
        // Process boolean values - you'll need to check your current state and currentKey
        return true;
    }

    bool null() override {
        // Process null values - you'll need to check your current state and currentKey
        return true;
    }

    bool parse_error(std::size_t position, const std::string& last_token, const json::exception& ex) override {
        // Implement error handling
        return false;
    }

    bool binary(binary_t& val) override {
        return true;
    }

    bool number_unsigned(number_unsigned_t val) {
        return true;
    }

    const std::vector<TweetData>& getTweets() const {
        return tweets;
    }

    void reset() {
        tweets.clear(); // 清除已经收集的推文数据
        inDoc = false;
        inData = false;
        isSentimentFound = false;
    }

    // Optionally, add methods to access your accumulated data
};



int main() {
    std::string file_path = "/Users/ruoyulu/CLionProjects/twitter-1mb.json"; // JSON 文件的路径
    std::ifstream input_file(file_path);

    if (!input_file.is_open()) {
        std::cerr << "Failed to open file." << std::endl;
        return -1; // 使用非零值表示程序遇到错误
    }

    CustomSAXHandler handler;

    std::string max_sentiment_hour;
    std::string max_sentiment_day;
    double max_sentiment_sum_hour = 0.0;
    double max_sentiment_sum_day = 0.0;
    std::string max_posts_hour;
    std::string max_posts_day;
    int max_posts_sum_hour = 0;
    int max_posts_sum_day = 0;

    std::vector<DataTime> data_hour;
    std::vector<DataTime> data_day;

    // 创建一个空的 JSON 对象，用于存储解析后的数据
    nlohmann::json tiny_json = nlohmann::json::object();

    while (input_file) { // 循环直到文件结束
        handler.reset(); // 重置handler状态，准备读取新的1000条数据
        bool success = json::sax_parse(input_file, &handler);

        if (!success) {
                std::cerr << "Failed to parse JSON." << std::endl;
        }

        // 访问解析后的数据
        const auto& tweets = handler.getTweets();

        std::cout<<"tweets.size:"<<tweets.size()<<std::endl;

        for (const auto& tweet : handler.getTweets()) {
                // 解析日期作为键
                std::string date_key = tweet.created_at.substr(0, 10);
                if (!tiny_json.contains(date_key)) {
                    tiny_json[date_key] = nlohmann::json::array();
                }
                nlohmann::json tweet_json = {
                        {"created_at", tweet.created_at},
                        {"sentiment", tweet.sentiment}
                };
                tiny_json[date_key].push_back(tweet_json);
        }

//        int count = 0;
//        for (const auto& item : tiny_json.items()) {
//            std::cout << item.key() << ": " << item.value().dump(2) << std::endl;
//            if (++count >= 5) break; // 限制输出的日期数量，例如这里只打印5个日期的数据
//        }

        if (tweets.size() == 50000) {
            // 处理完1000条数据后，清除数据，准备下一批
            continue;
        } else {
            // 如果解析的数据少于1000条，则表示已到文件末尾
            break;
        }
    }

    process_json(tiny_json, 0, 1, data_hour, data_day);

    auto max_hour_sentiment_it = std::max_element(data_hour.begin(), data_hour.end(),
                                                  [](const DataTime &a, const DataTime &b) {
                                                      return a.sentiment_score < b.sentiment_score;
                                                  });

    if (max_hour_sentiment_it != data_hour.end()) {
        std::cout << "Hour with the highest sentiment score: " << max_hour_sentiment_it->timeKey
                  << " | Sentiment score: " << max_hour_sentiment_it->sentiment_score << std::endl;
    }

    auto max_day_sentiment_it = std::max_element(data_day.begin(), data_day.end(),
                                                 [](const DataTime &a, const DataTime &b) {
                                                     return a.sentiment_score < b.sentiment_score;
                                                 });

    if (max_day_sentiment_it != data_day.end()) {
        std::cout << "Day with the highest sentiment score: " << max_day_sentiment_it->timeKey
                  << " | Sentiment score: " << max_day_sentiment_it->sentiment_score << std::endl;
    }

    auto max_hour_posts_it = std::max_element(data_hour.begin(), data_hour.end(),
                                              [](const DataTime &a, const DataTime &b) {
                                                  return a.posts_num < b.posts_num;
                                              });
    if (max_hour_posts_it != data_hour.end()) {
        std::cout << "Hour with the most posts: " << max_hour_posts_it->timeKey
                  << " | Number of posts: " << max_hour_posts_it->posts_num << std::endl;
    }

    auto max_day_posts_it = std::max_element(data_day.begin(), data_day.end(),
                                             [](const DataTime &a, const DataTime &b) {
                                                 return a.posts_num < b.posts_num;
                                             });
    if (max_day_posts_it != data_day.end()) {
        std::cout << "Day with the most posts: " << max_day_posts_it->timeKey
                  << " | Number of posts: " << max_day_posts_it->posts_num << std::endl;

    }

    return 0;
}
