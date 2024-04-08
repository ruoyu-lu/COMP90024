#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include "nlohmann/json.hpp"
#include <mpi.h>

using json = nlohmann::json;

void process_json(json& data, int rank, int size, std::string& max_sentiment_hour, std::string& max_sentiment_day,
                  double& max_sentiment_sum_hour, double& max_sentiment_sum_day, std::string& max_posts_hour,
                  std::string& max_posts_day, int& max_posts_sum_hour, int& max_posts_sum_day) {
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

        std::ostringstream hour_stream;
        hour_stream << std::put_time(&tm, "%Y-%m-%d-%H");
        std::string hour_key = hour_stream.str();

        sentiment_sum_by_hour[hour_key] += sentiment;
        sentiment_sum_by_day[hour_stream.str().substr(0, 10)] += sentiment;
        posts_sum_by_hour[hour_key]++;
        posts_sum_by_day[hour_stream.str().substr(0, 10)]++;

    }

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

    if(rank == 0) {
        ::process_json(data_set, rank, size, max_sentiment_hour, max_sentiment_day,
                       max_sentiment_sum_hour,max_sentiment_sum_day, max_posts_hour,
                       max_posts_day, max_posts_sum_hour, max_posts_sum_day);

        std::string receive_max_sentiment_hour;
        std::string receive_max_sentiment_day;
        double receive_max_sentiment_sum_hour = 0.0;
        double receive_max_sentiment_sum_day = 0.0;
        std::string receive_max_posts_hour;
        std::string receive_max_posts_day;
        int receive_max_posts_sum_hour = 0;
        int receive_max_posts_sum_day = 0;

        for(int j = 1; j < size; j++) {

            std::cout << "this is rank 00000000000000000" << std::endl;

            char temp_buffer[100];
            
            //recive data
            MPI_Recv(temp_buffer, 100, MPI_CHAR, j, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            receive_max_sentiment_hour = std::string(temp_buffer);
            MPI_Recv(temp_buffer, 100, MPI_CHAR, j, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            receive_max_sentiment_day = std::string(temp_buffer);
            MPI_Recv(&receive_max_sentiment_sum_hour, 1, MPI_DOUBLE, j, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            MPI_Recv(&receive_max_sentiment_sum_day, 1, MPI_DOUBLE, j, 0, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            MPI_Recv(temp_buffer, 100, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            receive_max_posts_hour = std::string(temp_buffer);
            MPI_Recv(temp_buffer, 100, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            receive_max_posts_day = std::string(temp_buffer);
            MPI_Recv(&receive_max_posts_sum_hour, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&receive_max_posts_sum_day, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            //compare data with recived data
            if (receive_max_sentiment_sum_hour > max_sentiment_sum_hour) {
                max_sentiment_sum_hour = receive_max_sentiment_sum_hour;
                max_sentiment_hour = receive_max_sentiment_hour;
            }
            if (receive_max_sentiment_sum_day > max_sentiment_sum_day) {
                max_sentiment_sum_day = receive_max_sentiment_sum_day;
                max_sentiment_day = receive_max_sentiment_day;
            }
            if (receive_max_posts_sum_hour > max_posts_sum_hour) {
                max_posts_sum_hour = receive_max_posts_sum_hour;
                max_posts_hour = receive_max_posts_hour;
            }
            if (receive_max_posts_sum_day > max_posts_sum_day) {
                max_posts_sum_day = receive_max_posts_sum_day;
                max_posts_day = receive_max_posts_day;
            }
        }

        //output
        std::cout << "hour with the highest sentiment sum: " << max_sentiment_hour
                  << " || Sentiment sum: " << std::fixed << std::setprecision(2) << max_sentiment_sum_hour << std::endl;
        std::cout << "day with the highest sentiment sum: " << max_sentiment_day
                  << " || Sentiment sum: " << std::fixed << std::setprecision(2) << max_sentiment_sum_day << std::endl;
        std::cout << "hour with the most posts: " << max_posts_hour
                  << " || Posts: " << max_posts_sum_hour << std::endl;
        std::cout << "day with the most posts: " << max_posts_day
                  << " || Posts: " << max_posts_sum_day << std::endl;

    }else {
        ::process_json(data_set, rank, size, max_sentiment_hour, max_sentiment_day,
                       max_sentiment_sum_hour,max_sentiment_sum_day, max_posts_hour,
                       max_posts_day, max_posts_sum_hour, max_posts_sum_day);

        std::cout<<"this is rank "<<rank<<std::endl;

        //send data
        MPI_Send(max_sentiment_hour.c_str(), max_sentiment_hour.size()+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        MPI_Send(max_sentiment_day.c_str(), max_sentiment_day.size()+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&max_sentiment_sum_hour, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&max_sentiment_sum_day, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        MPI_Send(max_posts_hour.c_str(), max_posts_hour.size()+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        MPI_Send(max_posts_day.c_str(), max_posts_day.size()+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&max_posts_sum_hour, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&max_posts_sum_day, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
