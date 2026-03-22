pkg load statistics

n = [1000, 5000, 10000];
gen_array = {'Мультипликативный конгруэнтный', ...
             'Фиббоначчи с запаздыванием', ...
             'Вихрь Мерсенна'};

% Метрики
fprintf('%-5s | %-39s | %-12s | %-12s | %-15s\n', ...
        'Объем', 'Генератор', 'Мат.ожидание', 'Дисперсия', 'Ср.кв.откл.');
fprintf('%s\n', repmat('-', 1, 90));

for i = 1:length(n)
    amount = n(i);

    for j = 1:length(gen_array)
        generator = gen_array{j};
        random_raw = lab1_generators(generator, amount);

        max_val = max(random_raw);
        min_val = min(random_raw);
        random = (random_raw - min_val) / (max_val - min_val);

        mean_val = round(mean(random));
        var_val = round(var(random));
        std_val = round(std(random));

        fprintf('%-5d | %-39s | %-12.3f | %-12.3f | %-15.3f\n', ...
                amount, generator, mean_val, var_val, std_val);
    end
end