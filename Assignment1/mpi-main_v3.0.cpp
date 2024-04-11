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

    int num_elements_hour, num_elements_day;
    char created_at[20];
    double sentiment;

    if(rank == 0) {
        std::vector<DataTime> data_hour;
        std::vector<DataTime> data_day;

        const std::string file_name = "twitter-50mb.json";
        std::ifstream file(file_name);
        std::string line;
        //跳过
        std::getline(file, line);
        int send_rank = 1;

        while (std::getline(file, line)) {
            if (!line.empty() && line == "{}]}") {
                break;
            }
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty() && line.back() == ',') {
                line.pop_back();
            }
            if (line.empty()) {
                std::cout << "empty line!!!" << std::endl;
            }

            json j;
            try {
                j = json::parse(line);
            } catch (json::parse_error &e) {
                std::cerr << "解析错误：" << e.what() << "\n在这一行：" << line << std::endl;
            }

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

            if (size > 1) {
                MPI_Send(created_at, 20, MPI_CHAR, send_rank, 0, MPI_COMM_WORLD);
                MPI_Send(&sentiment, 1, MPI_DOUBLE, send_rank, 0, MPI_COMM_WORLD);
                send_rank = send_rank % 7 + 1;
            }
        }
        for(int i = 1; i < size; i++){
            MPI_Send(created_at, 20, MPI_CHAR, i, 1, MPI_COMM_WORLD);
            MPI_Send(&sentiment, 1, MPI_DOUBLE, i, 1, MPI_COMM_WORLD);
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
            data_hour.insert(data_hour.end(), data_hour_recv.begin(), data_hour_recv.end());
            data_day.insert(data_day.end(), data_day_recv.begin(), data_day_recv.end());

            std::cout<<"Rank 0 received data from rank "<<i<<std::endl;
            std::cout<<data_day_recv.size()<<std::endl;
            std::cout<<data_hour_recv.size()<<std::endl;

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


        std::cout<< "Data processing is done!" << std::endl;
        std::cout<<data_day.size()<<std::endl;
        std::cout<<data_hour.size()<<std::endl;
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

        std::map<std::string, double> sentiment_sum_by_hour;
        std::map<std::string, double> sentiment_sum_by_day;
        std::map<std::string, int> posts_sum_by_hour;
        std::map<std::string,int> posts_sum_by_day;
        while(true){

            char created_at[20];
            double sentiment;

            MPI_Recv(created_at, 20, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            MPI_Recv(&sentiment, 1, MPI_DOUBLE, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

            if(status.MPI_TAG == 1){
                //push hour data to vector
                for (const auto &pair: sentiment_sum_by_hour) {
                    data_hour.emplace_back(pair.first, pair.second, posts_sum_by_hour[pair.first]);
                }

                //push day data to vector
                for (const auto &pair: sentiment_sum_by_day) {
                    data_day.emplace_back(pair.first, pair.second, posts_sum_by_day[pair.first]);
                }

                num_elements_hour = data_hour.size();
                num_elements_day = data_day.size();

                MPI_Send(&num_elements_hour, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
                MPI_Send(&num_elements_day, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

                //send data to rank 0
                MPI_Send(data_hour.data(), data_hour.size(), data_time_type, 0, 0, MPI_COMM_WORLD);
                MPI_Send(data_day.data(), data_day.size(), data_time_type, 0, 0, MPI_COMM_WORLD);

                break;
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

            sentiment_sum_by_hour[hour_key] += sentiment;
            sentiment_sum_by_day[day_key] += sentiment;
            posts_sum_by_hour[hour_key]++;
            posts_sum_by_day[day_key]++;

        }
    }
    MPI_Finalize();
    return 0;
}