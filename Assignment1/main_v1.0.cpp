#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include "nlohmann/json.hpp"

using json = nlohmann::json;

void process_data(json& data){
    std::map<std::string, double> sentiment_sum_by_hour;
    std::map<std::string, double> sentiment_sum_by_day;
    std::map<std::string, int> posts_sum_by_hour;
    std::map<std::string,int> posts_sum_by_day;

    for (const auto& item : data["rows"]) {
        if (!item.is_object() || !item.contains("doc") || !item["doc"].is_object()) {
            continue;
        }

        const auto& doc = item["doc"];
        if (!doc.contains("data") || !doc["data"].is_object()) {
            continue;
        }

        const auto& docData = doc["data"];
        if (!docData.contains("created_at") || !docData["created_at"].is_string()) {
            continue;
        }

        std::string created_at = docData["created_at"];
        std::tm tm = {};
        std::istringstream ss(created_at);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");

        if (ss.fail()) {
            std::cerr << "Failed to parse the created_at time: " << created_at << std::endl;
            continue;
        }

        double sentiment = 0; // Default to 0
        if (docData.contains("sentiment") && docData["sentiment"].is_number()) {
            sentiment = docData["sentiment"].get<double>();
        }

        // 构造日期时间键
        std::ostringstream hour_stream;
        hour_stream << std::put_time(&tm, "%Y-%m-%d-%H");
        std::string hour_key = hour_stream.str();

        // 累加数据
        sentiment_sum_by_hour[hour_key] += sentiment;
        sentiment_sum_by_day[hour_stream.str().substr(0, 10)] += sentiment;
        posts_sum_by_hour[hour_key]++;
        posts_sum_by_day[hour_stream.str().substr(0, 10)]++;

    }

    // 寻找最高情绪和最多帖子的日期时间
    double max_sentiment_sum_hour = 0.0;
    double max_sentiment_sum_day = 0.0;
    std::string max_sentiment_hour;
    std::string max_sentiment_day;
    int max_posts_sum_hour = 0;
    int max_posts_sum_day = 0;
    std::string max_posts_hour;
    std::string max_posts_day;

    for (const auto& kv : sentiment_sum_by_hour) {
        if (kv.second > max_sentiment_sum_hour) {
            max_sentiment_sum_hour = kv.second;
            max_sentiment_hour = kv.first;
        }
    }

    for (const auto& kv : sentiment_sum_by_day) {
        if (kv.second > max_sentiment_sum_day) {
            max_sentiment_sum_day = kv.second;
            max_sentiment_day = kv.first;
        }
    }

    for (const auto& kv : posts_sum_by_hour) {
        if (kv.second > max_posts_sum_hour) {
            max_posts_sum_hour = kv.second;
            max_posts_hour = kv.first;
        }
    }

    for(const auto& kv : posts_sum_by_day){
        if(kv.second > max_posts_sum_day){
            max_posts_sum_day = kv.second;
            max_posts_day = kv.first;
        }
    }

    // 输出结果
    std::cout << "hour with the highest sentiment sum: " << max_sentiment_hour
              << " || Sentiment sum: " << std::fixed << std::setprecision(2) << max_sentiment_sum_hour << std::endl;
    std::cout << "day with the highest sentiment sum: " << max_sentiment_day
                << " || Sentiment sum: " << std::fixed << std::setprecision(2) << max_sentiment_sum_day << std::endl;
    std::cout << "hour with the most posts: " << max_posts_hour
              << " || Posts: " << max_posts_sum_hour << std::endl;
    std::cout << "day with the most posts: " << max_posts_day
                << " || Posts: " << max_posts_sum_day << std::endl;
}

int main() {
    std::ifstream f("/Users/ruoyulu/CLionProjects/twitter-50mb.json");
    if (!f.is_open()) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    json full_data;
    try {
        f >> full_data;
    } catch (json::parse_error& e) {
        std::cerr << "Parse error: " << e.what() << '\n';
        f.close();
        return 1;
    }
    f.close();

    process_data(full_data);

    return 0;
}
