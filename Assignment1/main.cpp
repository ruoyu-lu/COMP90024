
#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include "nlohmann/json.hpp"

using json = nlohmann::json;

int main() {

    // Read json file
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

    if (!full_data.is_object() || !full_data.contains("rows") || !full_data["rows"].is_array()) {
        std::cerr << "JSON structure is not as expected." << std::endl;
        return 1;
    }

    std::map<int, double> sentiment_sum_by_hour;
    std::map<int, double> sentiment_sum_by_day;
    std::map<int, int> posts_per_hour;
    std::map<int, int> posts_per_day;

    std::tm tm_1 = {};

    for (const auto& item : full_data["rows"]) {
        if (!item.is_object() || !item.contains("doc") || !item["doc"].is_object()) {
            continue;
        }

        const auto& doc = item["doc"];
        if (!doc.contains("data") || !doc["data"].is_object()) {
            std::cerr << "'data' object is missing or is not an object" << std::endl;
            continue;
        }

        const auto& data = doc["data"];
        if (!data.contains("created_at") || !data["created_at"].is_string()) {
            std::cerr << "'created_at' field is missing or is not a string" << std::endl;
            continue;
        }

        std::string created_at = data["created_at"];
        std::tm tm = {};
        std::istringstream ss(created_at);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");
        tm_1 = tm;

        if (ss.fail()) {
            std::cerr << "Failed to parse the created_at time: " << created_at << std::endl;
            continue;
        }

        double sentiment = 0; // Default to 0
        if (data.contains("sentiment") && data["sentiment"].is_number()) {
            sentiment = data["sentiment"].get<double>();
        } // No else needed as sentiment is already initialized to 0

        sentiment_sum_by_hour[tm.tm_hour] += sentiment;
        sentiment_sum_by_day[tm.tm_mday] += sentiment;
        ++posts_per_hour[tm.tm_hour];
        ++posts_per_day[tm.tm_mday];
    }

    int max_hour = -1;
    double max_sentiment_sum = 0.0;
    for (const auto& kv : sentiment_sum_by_hour) {
        if (max_hour == -1 || kv.second > max_sentiment_sum) {
            max_hour = kv.first;
            max_sentiment_sum = kv.second;
        }
    }


    std::string time_period = "AM";
    if (max_hour != -1) {
        int next_hour = (max_hour + 1) % 24;
        if(max_hour>=12){
            max_hour = max_hour - 12;
            time_period = "PM";
        }

        //the happiest hour ever
        std::cout << "Hour period with the highest sentiment sum: "
                  << std::put_time(&tm_1, "%Y-%m-%d ") << max_hour << "-" << next_hour << time_period;
        std::cout << std::fixed << std::setprecision(17) << " || Sentiment sum: " << max_sentiment_sum << std::endl;

        //the happiest day ever
        std::cout << "Day period with the highest sentiment sum: "
                  << std::put_time(&tm_1, "%Y-%m-%d ");
        std::cout << std::fixed << std::setprecision(17) << " || Sentiment sum: "
                  << sentiment_sum_by_day[tm_1.tm_mday] << std::endl;


        max_hour = -1;
        int max_posts = 0;
        for (const auto& hour_posts : posts_per_hour) {
            if (hour_posts.second > max_posts) {
                max_hour = hour_posts.first;
                max_posts = hour_posts.second;
            }
        }

        time_period = "AM";
        if (max_hour != -1) {
            next_hour = (max_hour + 1) % 24;
            if (max_hour >= 12) {
                max_hour = max_hour - 12;
                time_period = "PM";
            }
        }

        //the most active hour ever
        std::cout << "Hour with the most posts is: " << std::put_time(&tm_1, "%Y-%m-%d ")
        << max_hour << "-" << next_hour << time_period << " with " << max_posts << " posts." << std::endl;

        //the most active day ever
        std::cout<< "Day with the most posts is: " << std::put_time(&tm_1, "%Y-%m-%d ") << " with "
        << posts_per_day[tm_1.tm_mday] << " posts." << std::endl;


    } else {
        std::cout << "No sentiment data was found." << std::endl;
    }


    return 0;
}



