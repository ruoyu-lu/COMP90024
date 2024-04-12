#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include "nlohmann/json.hpp"
#include <mpi.h>

using json = nlohmann::json;

struct DataTime {
    char timeKey[20];
    double sentiment_score;
    int posts_num;

    DataTime() : sentiment_score(0), posts_num(0) {
        timeKey[0] = '\0'; // 初始化为空字符串
    }

    DataTime(const std::string& key, double score, int num)
            : sentiment_score(score), posts_num(num) {
        std::strncpy(timeKey, key.c_str(), sizeof(timeKey) - 1);
        timeKey[sizeof(timeKey) - 1] = '\0';
    }
};

int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Get the number of processes
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Get the rank of the process

    // Define a new MPI datatype for DataTime
    MPI_Datatype data_time_type;
    int blocklengths[3] = {20, 1, 1};
    MPI_Datatype types[3] = {MPI_CHAR, MPI_DOUBLE, MPI_INT};
    MPI_Aint offsets[3];
    offsets[0] = offsetof(DataTime, timeKey);
    offsets[1] = offsetof(DataTime, sentiment_score);
    offsets[2] = offsetof(DataTime, posts_num);
    MPI_Type_create_struct(3, blocklengths, offsets, types, &data_time_type);
    MPI_Type_commit(&data_time_type);

    std::string max_sentiment_hour;
    std::string max_sentiment_day;
    double max_sentiment_sum_hour = 0.0;
    double max_sentiment_sum_day = 0.0;
    std::string max_posts_hour;
    std::string max_posts_day;
    int max_posts_sum_hour = 0;
    int max_posts_sum_day = 0;

    char created_at[20];
    double sentiment;

    if(rank == 0) {
        std::vector<DataTime> data_hour;
        std::vector<DataTime> data_day;

        int num_elements_hour, num_elements_day;

        const std::string file_name = "twitter-100gb.json";
        std::ifstream file(file_name);
        std::string line;
        if (!file.is_open()) {
            std::cerr << "Failed to open the file." << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        //跳过
        std::getline(file, line);
        int send_rank = 1;
        int line_count = rank;
        int i = 1;

        while (std::getline(file, line)) {
            if(size == 1) {
                line.pop_back();
                line.pop_back();

                std::cout << "rank " << rank << " Processing line " << i << std::endl;
                i++;
                json j;
                j = json::parse(line);

                if (!j.is_object() || !j.contains("doc") || !j["doc"].is_object()) {
                    continue;
                }
                const auto &doc = j["doc"];
                if (!doc.contains("data") || !doc["data"].is_object()) {
                    continue;
                }
                const auto &docData = doc["data"];
                if (!docData.contains("created_at") || !docData["created_at"].is_string()) {
                    continue;
                }

                std::string created_at_string = docData["created_at"];
                std::strncpy(created_at, created_at_string.c_str(), sizeof(created_at) - 1);

                sentiment = 0; // Default to 0
                if (docData.contains("sentiment") && docData["sentiment"].is_number()) {
                    sentiment = docData["sentiment"].get<double>();
                }
                std::tm tm = {};
                std::istringstream ss(created_at);
                ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");

                if (ss.fail()) {
                    std::cerr << "Failed to parse the created_at time: " << created_at << std::endl;
                }

                std::ostringstream os;
                os << std::put_time(&tm, "%Y-%m-%d-%H");
                std::string hour_key = os.str(); //generate hour_key

                os.str(""); // 清空ostringstream
                os << std::put_time(&tm, "%Y-%m-%d");
                std::string day_key = os.str(); //generate day_key

                auto it_hour = std::find_if(data_hour.begin(), data_hour.end(), [&hour_key](const DataTime &a) {
                    return std::string(a.timeKey) == hour_key;
                });

                if (it_hour != data_hour.end()) {
                    it_hour->sentiment_score += sentiment;
                    it_hour->posts_num++;
                } else {
                    data_hour.emplace_back(hour_key, sentiment, 1);
                }

                auto it_day = std::find_if(data_day.begin(), data_day.end(), [&day_key](const DataTime &a) {
                    return std::string(a.timeKey) == day_key;
                });

                if (it_day != data_day.end()) {
                    it_day->sentiment_score += sentiment;
                    it_day->posts_num++;
                } else {
                    data_day.emplace_back(day_key, sentiment, 1);
                }
            }else{
                if (line_count % 8 == 0) {
                    line.pop_back();
                    line.pop_back();

                    json j;
                    j = json::parse(line);

                    if (!j.is_object() || !j.contains("doc") || !j["doc"].is_object()) {
                        continue;
                    }
                    const auto &doc = j["doc"];
                    if (!doc.contains("data") || !doc["data"].is_object()) {
                        continue;
                    }
                    const auto &docData = doc["data"];
                    if (!docData.contains("created_at") || !docData["created_at"].is_string()) {
                        continue;
                    }

                    std::string created_at_string = docData["created_at"];
                    std::strncpy(created_at, created_at_string.c_str(), sizeof(created_at) - 1);

                    sentiment = 0; // Default to 0
                    if (docData.contains("sentiment") && docData["sentiment"].is_number()) {
                        sentiment = docData["sentiment"].get<double>();
                    }
                    std::tm tm = {};
                    std::istringstream ss(created_at);
                    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

                    if (ss.fail()) {
                        std::cerr << "Failed to parse the created_at time: " << created_at << std::endl;
                    }

                    std::ostringstream os;
                    os << std::put_time(&tm, "%Y-%m-%d-%H");
                    std::string hour_key = os.str(); //generate hour_key

                    os.str(""); // 清空ostringstream
                    os << std::put_time(&tm, "%Y-%m-%d");
                    std::string day_key = os.str(); //generate day_key

                    auto it_hour = std::find_if(data_hour.begin(), data_hour.end(), [&hour_key](const DataTime &a) {
                        return std::string(a.timeKey) == hour_key;
                    });

                    if (it_hour != data_hour.end()) {
                        it_hour->sentiment_score += sentiment;
                        it_hour->posts_num++;
                    } else {
                        data_hour.emplace_back(hour_key, sentiment, 1);
                    }

                    auto it_day = std::find_if(data_day.begin(), data_day.end(), [&day_key](const DataTime &a) {
                        return std::string(a.timeKey) == day_key;
                    });

                    if (it_day != data_day.end()) {
                        it_day->sentiment_score += sentiment;
                        it_day->posts_num++;
                    } else {
                        data_day.emplace_back(day_key, sentiment, 1);
                    }
                }
                line_count++;
            }
        }

        std::vector<DataTime> data_hour_recv;
        std::vector<DataTime> data_day_recv;
        //receive data from other ranks
        for (int i = 1; i < size; i++) {
            // 假设发送方首先发送了一个整数，表示将要发送的DataTime结构体数量
            MPI_Recv(&num_elements_hour, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&num_elements_day, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // 根据接收到的数量调整vector的大小，以准备接收数据
            data_hour_recv.resize(num_elements_hour);
            data_day_recv.resize(num_elements_day);

            MPI_Recv(data_hour_recv.data(), num_elements_hour, data_time_type, i, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            MPI_Recv(data_day_recv.data(), num_elements_day, data_time_type, i, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);

            //combine data from other ranks
            for (const auto &item: data_hour_recv) {
                auto it = std::find_if(data_hour.begin(), data_hour.end(), [&item](const DataTime &a) {
                    return std::string(a.timeKey) == std::string(item.timeKey);
                });
                if (it != data_hour.end()) {
                    it->sentiment_score += item.sentiment_score;
                    it->posts_num += item.posts_num;
                } else {
                    data_hour.push_back(item);
                }
            }

            for (const auto &item: data_day_recv) {
                auto it = std::find_if(data_day.begin(), data_day.end(), [&item](const DataTime &a) {
                    return std::string(a.timeKey) == std::string(item.timeKey);
                });
                if (it != data_day.end()) {
                    it->sentiment_score += item.sentiment_score;
                    it->posts_num += item.posts_num;
                } else {
                    data_day.push_back(item);
                }
            }
        }

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
        MPI_Status status;
        int signal;
        std::vector<DataTime> data_hour;
        std::vector<DataTime> data_day;

        int num_elements_hour, num_elements_day;

        const std::string file_name = "twitter-100gb.json";

        std::ifstream file(file_name);
        std::string line;
        if (!file.is_open()) {
            std::cerr << "Failed to open the file." << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        //跳过
        std::getline(file, line);
        int send_rank = 1;
        int line_count = rank;

        while (std::getline(file, line)) {
            if (line_count % 8 == 0) {
                line.pop_back();
                line.pop_back();

                json j;
                j = json::parse(line);

                if (!j.is_object() || !j.contains("doc") || !j["doc"].is_object()) {
                    continue;
                }
                const auto &doc = j["doc"];
                if (!doc.contains("data") || !doc["data"].is_object()) {
                    continue;
                }
                const auto &docData = doc["data"];
                if (!docData.contains("created_at") || !docData["created_at"].is_string()) {
                    continue;
                }

                std::string created_at_string = docData["created_at"];
                std::strncpy(created_at, created_at_string.c_str(), sizeof(created_at) - 1);

                sentiment = 0; // Default to 0
                if (docData.contains("sentiment") && docData["sentiment"].is_number()) {
                    sentiment = docData["sentiment"].get<double>();
                }
                std::tm tm = {};
                std::istringstream ss(created_at);
                ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

                if (ss.fail()) {
                    std::cerr << "Failed to parse the created_at time: " << created_at << std::endl;
                }

                std::ostringstream os;
                os << std::put_time(&tm, "%Y-%m-%d-%H");
                std::string hour_key = os.str(); //generate hour_key

                os.str(""); // 清空ostringstream
                os << std::put_time(&tm, "%Y-%m-%d");
                std::string day_key = os.str(); //generate day_key

                auto it_hour = std::find_if(data_hour.begin(), data_hour.end(), [&hour_key](const DataTime &a) {
                    return std::string(a.timeKey) == hour_key;
                });

                if (it_hour != data_hour.end()) {
                    it_hour->sentiment_score += sentiment;
                    it_hour->posts_num++;
                } else {
                    data_hour.emplace_back(hour_key, sentiment, 1);
                }

                auto it_day = std::find_if(data_day.begin(), data_day.end(), [&day_key](const DataTime &a) {
                    return std::string(a.timeKey) == day_key;
                });

                if (it_day != data_day.end()) {
                    it_day->sentiment_score += sentiment;
                    it_day->posts_num++;
                } else {
                    data_day.emplace_back(day_key, sentiment, 1);
                }
            }
            line_count++;
        }
        num_elements_hour = data_hour.size();
        num_elements_day = data_day.size();

        MPI_Send(&num_elements_hour, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&num_elements_day, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

        //send data to rank 0
        MPI_Send(data_hour.data(), data_hour.size(), data_time_type, 0, 0, MPI_COMM_WORLD);
        MPI_Send(data_day.data(), data_day.size(), data_time_type, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Finalize();
    return 0;
}
