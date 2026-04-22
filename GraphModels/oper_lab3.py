import numpy as np

phi = np.array([
    [0.2, 0.4, 0.8, 1.0],
    [0.4, 0.5, 0.9, 1.0],
    [0.6, 0.6, 1.1, 1.1],
    [0.8, 0.7, 1.3, 1.1],
    [1.0, 0.8, 1.4, 1.2],
    [1.2, 0.9, 1.5, 1.2],
    [1.2, 1.0, 1.5, 1.3]
])

n = phi.shape[1]
m = phi.shape[0]
total = 7

dp = np.zeros((total + 1, n + 1))
choice = np.zeros((total + 1, n + 1), dtype=int)

for j in range(1, n + 1):
    for x in range(total + 1):
        for k in range(x + 1):
            if k <= m:
                val = dp[x - k][j - 1] + phi[k - 1][j - 1] if k > 0 else dp[x][j - 1]
                if val > dp[x][j]:
                    dp[x][j] = val
                    choice[x][j] = k

print(f"Таблица промежуточной оптимизации\n{dp}")
print(f"Таблица выборов\n{choice}")

print("Ресурсы")
x_remaining = total
for j in range(n, 0, -1):
    k = choice[x_remaining][j]
    print(f"Объект {j}: {k}")
    x_remaining -= k

print("Максимальный суммарный эффект:", dp[total][n])