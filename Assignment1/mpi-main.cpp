#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include "nlohmann/json.hpp"
#include <mpi.h>

using json = nlohmann::json;

struct DataTime {
    std::string timeKey;
    double sentiment_score;
    int posts_num;

    DataTime(std::string key, double score, int num)
            : timeKey(std::move(key)), sentiment_score(score), posts_num(num) {}
};

void process_json(json& data, int rank, int size, std::vector<DataTime> &data_hour, std::vector<DataTime> &data_day) {

    std::map<std::string, double> sentiment_sum_by_hour;
    std::map<std::string, double> sentiment_sum_by_day;
    std::map<std::string, int> posts_sum_by_hour;
    std::map<std::string,int> posts_sum_by_day;

    int start = (data["rows"].size() / size) * rank;
    int end = (data["rows"].size() / size) * (rank + 1);
    for (int i = start; i < end; ++i) {

        const auto& item = data["rows"][i];

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

        std::ostringstream os;
        os << std::put_time(&tm, "%Y-%m-%d-%H");
        std::string hour_key = os.str(); //generate hour_key

        os.str(""); // 清空ostringstream
        os << std::put_time(&tm, "%Y-%m-%d");
        std::string day_key = os.str(); //generate day_key

        sentiment_sum_by_hour[hour_key] += sentiment;
        sentiment_sum_by_day[day_key] += sentiment;
        posts_sum_by_hour[hour_key]++;
        posts_sum_by_day[day_key]++;
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

int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Get the number of processes
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Get the rank of the process

    std::ifstream f("twitter-50mb.json");
    if (!f.is_open()) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }
    json data_set;
    try {
        f >> data_set;
    } catch (json::parse_error &e) {
        std::cerr << "Parse error: " << e.what() << '\n';
        f.close();
        return 1;
    }
    f.close();

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

    if(rank == 0) {
        process_json(data_set, rank, size, data_hour, data_day);

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

    }else {
        process_json(data_set, rank, size, data_hour, data_day);

    }

    MPI_Finalize();
    return 0;
}
