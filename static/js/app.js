const state = {
    dashboard: null,
    charts: {},
    currentRoom: "general",
    currentPanel: "overview",
    theme: "sunrise",
    sidebarCollapsed: false,
    session: null,
    permissions: {},
};

const SALE_FLOW = ["new", "proposal", "negotiation", "won"];
const STATUS_LABELS = {
    active: "Активный",
    approved: "Согласован",
    client: "Клиент",
    contract: "Договор",
    done: "Готово",
    draft: "Черновик",
    document: "Документ",
    high: "Высокий",
    in_progress: "В работе",
    invoice: "Счет",
    low: "Низкий",
    medium: "Средний",
    negotiation: "Переговоры",
    new: "Новая",
    onboarding: "Подключение",
    open: "Открыта",
    proposal: "Предложение",
    prospect: "Потенциальный",
    sale: "Сделка",
    sent: "Отправлен",
    task: "Задача",
    vip: "VIP",
    won: "Успешно",
};
const ROOM_LABELS = {
    general: "Общий канал",
    contracts: "Договоры",
    support: "Поддержка",
};
const NOTE_STORAGE_KEY = "sales_hub_local_notes_v2";
const THEME_STORAGE_KEY = "sales_hub_theme_v1";
const SIDEBAR_STORAGE_KEY = "sales_hub_sidebar_state_v1";
const AVAILABLE_THEMES = ["sunrise", "ocean", "graphite", "forest"];

function readStorage(key) {
    try {
        return window.localStorage.getItem(key);
    } catch (error) {
        return null;
    }
}

function writeStorage(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch (error) {
        // Ignore storage issues and continue with in-memory state.
    }
}

function normalizeTheme(theme) {
    return AVAILABLE_THEMES.includes(theme) ? theme : "sunrise";
}

function cssVar(name, fallback) {
    const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return value || fallback;
}

function getChartTheme() {
    return {
        legend: cssVar("--chart-legend", "#4b5d73"),
        grid: cssVar("--chart-grid", "rgba(43, 58, 77, 0.08)"),
        primary: {
            border: cssVar("--chart-primary", "#12616a"),
            fill: cssVar("--chart-primary-fill", "rgba(18, 97, 106, 0.18)"),
        },
        secondary: {
            border: cssVar("--chart-secondary", "#e28c34"),
            fill: cssVar("--chart-secondary-fill", "rgba(226, 140, 52, 0.58)"),
        },
        accent: {
            border: cssVar("--chart-accent", "#d55f4b"),
            fill: cssVar("--chart-accent-fill", "rgba(213, 95, 75, 0.48)"),
        },
        doughnutBorder: [
            cssVar("--chart-doughnut-1", "#12616a"),
            cssVar("--chart-doughnut-2", "#e28c34"),
            cssVar("--chart-doughnut-3", "#2f8c6b"),
        ],
        doughnutFill: [
            cssVar("--chart-doughnut-1-fill", "rgba(18, 97, 106, 0.88)"),
            cssVar("--chart-doughnut-2-fill", "rgba(226, 140, 52, 0.82)"),
            cssVar("--chart-doughnut-3-fill", "rgba(47, 140, 107, 0.8)"),
        ],
    };
}

function escapeHtml(value) {
    return String(value === undefined || value === null ? "" : value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function formatCurrency(value) {
    return new Intl.NumberFormat("ru-RU", {
        maximumFractionDigits: 0,
    }).format(Number(value || 0));
}

function statusText(status) {
    return STATUS_LABELS[status] || ROOM_LABELS[status] || status || "Без статуса";
}

function hasPermission(name) {
    return Boolean(state.permissions && state.permissions[name]);
}

function statusBadge(status, extraClass = "") {
    const safeStatus = escapeHtml(status || "unknown");
    const safeClass = extraClass ? ` ${escapeHtml(extraClass)}` : "";
    return `<span class="badge status-${safeStatus}${safeClass}">${escapeHtml(statusText(status))}</span>`;
}

function formatDaysLeft(daysLeft) {
    if (daysLeft < 0) {
        return `Просрочено на ${Math.abs(daysLeft)} дн.`;
    }
    if (daysLeft === 0) {
        return "Сегодня";
    }
    if (daysLeft === 1) {
        return "1 день";
    }
    return `${daysLeft} дн.`;
}

function showNotice(message) {
    const notice = document.getElementById("notice");
    notice.textContent = message;
    notice.classList.add("show");
    window.clearTimeout(showNotice.timer);
    showNotice.timer = window.setTimeout(() => {
        notice.classList.remove("show");
    }, 3200);
}

async function api(path, options = {}) {
    const config = {
        headers: {
            "Content-Type": "application/json",
        },
        ...options,
    };

    if (!config.body) {
        delete config.headers["Content-Type"];
    }

    const response = await fetch(path, config);
    const data = await response.json();
    if (response.status === 401) {
        window.location.href = "/login?next=" + encodeURIComponent(window.location.pathname + window.location.hash);
        throw new Error(data.message || "Требуется вход в систему.");
    }
    if (!response.ok) {
        throw new Error(data.message || "Произошла ошибка при обращении к серверу.");
    }
    return data;
}

function formToObject(form) {
    return Object.fromEntries(new FormData(form).entries());
}

function emptyState(text) {
    return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

function hasWorkspaceNavigation() {
    return Boolean(document.querySelector("[data-workspace-tab]"));
}

function setActivePanel(panelName, updateHash = true) {
    if (!hasWorkspaceNavigation()) {
        return;
    }

    const knownPanels = ["overview", "clients", "sales", "documents", "communications"];
    const target = knownPanels.includes(panelName) ? panelName : "overview";
    state.currentPanel = target;

    document.querySelectorAll("[data-workspace-tab]").forEach((button) => {
        button.classList.toggle("is-active", button.dataset.workspaceTab === target);
    });
    document.querySelectorAll("[data-panel]").forEach((panel) => {
        panel.classList.toggle("is-active", panel.dataset.panel === target);
    });

    if (updateHash) {
        window.history.replaceState(null, "", `#${target}`);
    }

    const workspaceMain = document.querySelector(".workspace-main");
    if (workspaceMain) {
        workspaceMain.scrollTo({ top: 0, behavior: "smooth" });
    } else {
        window.scrollTo({ top: 0, behavior: "smooth" });
    }
}

function updateSidebarToggleButtons() {
    const label = state.sidebarCollapsed ? "Показать левую панель" : "Скрыть левую панель";
    document.querySelectorAll("[data-toggle-sidebar]").forEach((button) => {
        button.setAttribute("aria-label", label);
        button.setAttribute("aria-expanded", String(!state.sidebarCollapsed));
        button.classList.toggle("is-collapsed", state.sidebarCollapsed);
    });

    document.querySelectorAll("[data-sidebar-toggle-text]").forEach((node) => {
        node.textContent = label;
    });

    document.querySelectorAll(".sidebar-icon-button__text").forEach((node) => {
        node.textContent = state.sidebarCollapsed ? "Открыть" : "Панель";
    });
}

function setSidebarCollapsed(collapsed) {
    state.sidebarCollapsed = Boolean(collapsed);
    document.documentElement.dataset.sidebarState = state.sidebarCollapsed ? "collapsed" : "expanded";
    const shell = document.querySelector(".workspace-shell");
    if (shell) {
        shell.classList.toggle("is-sidebar-collapsed", state.sidebarCollapsed);
    }
    writeStorage(SIDEBAR_STORAGE_KEY, state.sidebarCollapsed ? "collapsed" : "expanded");
    updateSidebarToggleButtons();
}

function applyTheme(theme, options = {}) {
    const nextTheme = normalizeTheme(theme);
    state.theme = nextTheme;
    document.documentElement.dataset.theme = nextTheme;
    writeStorage(THEME_STORAGE_KEY, nextTheme);

    document.querySelectorAll("[data-theme-option]").forEach((button) => {
        const isActive = button.dataset.themeOption === nextTheme;
        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-pressed", String(isActive));
    });

    if (!options.skipChartRefresh && state.dashboard && state.dashboard.charts) {
        renderCharts(state.dashboard.charts);
    }
}

function renderSummary(summary) {
    const cards = [
        { label: "Выручка", value: `${formatCurrency(summary.revenue)} KZT` },
        { label: "Pipeline", value: `${formatCurrency(summary.pipeline)} KZT` },
        { label: "Прогноз", value: `${formatCurrency(summary.forecast)} KZT` },
        { label: "Клиенты", value: summary.active_clients },
        { label: "Конверсия", value: `${summary.conversion}%` },
        { label: "Средний чек", value: `${formatCurrency(summary.avg_deal)} KZT` },
        { label: "Срочные задачи", value: summary.urgent_tasks },
        { label: "Согласовано", value: summary.approved_documents },
    ];

    document.getElementById("summaryCards").innerHTML = cards
        .map(
            (card) => `
                <article class="summary-card">
                    <span>${escapeHtml(card.label)}</span>
                    <strong>${escapeHtml(card.value)}</strong>
                </article>
            `,
        )
        .join("");
}

function renderSessionChrome() {
    const badge = document.getElementById("notificationBadge");
    if (!badge || !state.session) {
        return;
    }
    const count = Number(state.session.notification_count || 0);
    badge.textContent = count ? `${count} сигналов` : "Все спокойно";
    badge.classList.toggle("is-calm", count === 0);
}

function renderSpotlight() {
    const container = document.getElementById("spotlightPanel");
    if (!container || !state.dashboard || !state.dashboard.spotlight) {
        return;
    }

    const spotlight = state.dashboard.spotlight;
    container.innerHTML = `
        <div class="section-head">
            <div>
                <p class="eyebrow">Персональный радар</p>
                <h3>${escapeHtml(spotlight.headline)}</h3>
            </div>
            <span class="pulse-chip">${escapeHtml(spotlight.badge)}</span>
        </div>
        <div class="spotlight-grid">
            <article class="spotlight-main">
                <div class="spotlight-copy">
                    <span class="spotlight-kicker">${escapeHtml(spotlight.role_label)}</span>
                    <strong>${escapeHtml(spotlight.name)}</strong>
                    <p>${escapeHtml(spotlight.mission)}</p>
                </div>
                <div class="spotlight-focus">
                    <span>Фокус сейчас</span>
                    <strong>${escapeHtml(spotlight.focus)}</strong>
                </div>
            </article>
            <article class="spotlight-score">
                <div class="score-orb">
                    <span>${escapeHtml(spotlight.score)}</span>
                </div>
                <div class="spotlight-metrics">
                    <div>
                        <span>Темп кабинета</span>
                        <strong>${escapeHtml(spotlight.badge)}</strong>
                    </div>
                    <div>
                        <span>Серия активности</span>
                        <strong>${escapeHtml(spotlight.streak)} дн.</strong>
                    </div>
                </div>
            </article>
        </div>
    `;
}

function renderNotifications() {
    const container = document.getElementById("notificationsPanel");
    if (!container) {
        return;
    }
    const items = state.dashboard && state.dashboard.notifications ? state.dashboard.notifications : [];
    if (!items.length) {
        container.innerHTML = emptyState("Сейчас нет срочных сигналов. Можно спокойно работать по плану.");
        return;
    }

    container.innerHTML = items
        .map((item) => `
            <article class="notice-item tone-${escapeHtml(item.tone)}">
                <div class="notice-item-top">
                    <strong>${escapeHtml(item.title)}</strong>
                    <span>${escapeHtml(item.meta)}</span>
                </div>
                <p>${escapeHtml(item.description)}</p>
                <button class="action-button" type="button" data-action="jump-panel" data-panel="${escapeHtml(item.panel)}">Открыть раздел</button>
            </article>
        `)
        .join("");
}

function renderQuickActions() {
    const container = document.getElementById("launchpadPanel");
    if (!container) {
        return;
    }
    const items = state.dashboard && state.dashboard.quick_actions ? state.dashboard.quick_actions : [];
    if (!items.length) {
        container.innerHTML = emptyState("Быстрые сценарии появятся, когда для вашей роли будет доступно больше действий.");
        return;
    }

    container.innerHTML = items
        .map((item) => `
            <article class="launchpad-item">
                <strong>${escapeHtml(item.title)}</strong>
                <p>${escapeHtml(item.description)}</p>
                <button class="secondary-button" type="button" data-action="jump-panel" data-panel="${escapeHtml(item.panel)}" data-focus="${escapeHtml(item.focus)}">Запустить</button>
            </article>
        `)
        .join("");
}

function focusTarget(selector) {
    if (!selector) {
        return;
    }
    const target = document.querySelector(selector);
    if (!target) {
        return;
    }
    target.scrollIntoView({ behavior: "smooth", block: "center" });
    window.setTimeout(() => {
        target.focus();
    }, 180);
}

function applyUserContext() {
    const user = state.session && state.session.user ? state.session.user : null;
    if (!user) {
        return;
    }

    const saleOwner = document.querySelector('#saleForm [name="owner"]');
    const taskOwner = document.querySelector('#taskForm [name="owner"]');
    const chatName = document.querySelector('#chatForm [name="sender_name"]');
    const chatRole = document.querySelector('#chatForm [name="sender_role"]');
    const chatClientField = document.querySelector('#chatForm select[name="client_id"]');
    const chatClientWrapper = chatClientField ? chatClientField.closest("label") : null;
    const chatClientSelect = document.getElementById("chatClientSelect");

    if (saleOwner && !saleOwner.value) {
        saleOwner.value = user.display_name || "";
    }
    if (taskOwner && !taskOwner.value) {
        taskOwner.value = user.display_name || "";
    }
    if (chatName) {
        chatName.value = user.display_name || "";
    }
    if (chatRole) {
        chatRole.value = user.role || "manager";
    }
    if (hasPermission("client_scoped")) {
        if (chatClientSelect && state.dashboard.clients.length) {
            chatClientSelect.value = String(state.dashboard.clients[0].id);
        }
        if (chatClientWrapper) {
            chatClientWrapper.classList.add("is-hidden");
        }
    } else if (chatClientWrapper) {
        chatClientWrapper.classList.remove("is-hidden");
    }
}

function createChart(key, canvasId, type, labels, values, colors) {
    if (!window.Chart) {
        return;
    }

    const canvas = document.getElementById(canvasId);
    if (!canvas) {
        return;
    }

    if (state.charts[key]) {
        state.charts[key].destroy();
    }

    const chartTheme = getChartTheme();
    state.charts[key] = new Chart(canvas, {
        type,
        data: {
            labels,
            datasets: [
                {
                    data: values,
                    borderColor: colors.border,
                    backgroundColor: colors.fill,
                    borderWidth: 2,
                    tension: 0.35,
                    fill: type === "line",
                },
            ],
        },
        options: {
            maintainAspectRatio: false,
            responsive: true,
            plugins: {
                legend: {
                    display: type === "doughnut",
                    labels: {
                        color: chartTheme.legend,
                    },
                },
                tooltip: {
                    callbacks: {
                        label(context) {
                            const value = context.raw || 0;
                            if (canvasId === "funnelChart" || canvasId === "documentChart") {
                                return `${context.label}: ${value}`;
                            }
                            return `${context.label}: ${formatCurrency(value)} KZT`;
                        },
                    },
                },
            },
            scales: type === "doughnut"
                ? {}
                : {
                    x: {
                        ticks: { color: chartTheme.legend },
                        grid: { display: false },
                    },
                    y: {
                        ticks: {
                            color: chartTheme.legend,
                            callback(value) {
                                return canvasId === "funnelChart" ? value : formatCurrency(value);
                            },
                        },
                        grid: { color: chartTheme.grid },
                    },
                },
        },
    });
}

function renderCharts(charts) {
    const palette = getChartTheme();

    createChart("revenue", "revenueChart", "line", charts.monthly_revenue.labels, charts.monthly_revenue.values, palette.primary);
    createChart("product", "productChart", "bar", charts.revenue_by_product.labels, charts.revenue_by_product.values, palette.secondary);
    createChart("funnel", "funnelChart", "bar", charts.sales_funnel.labels.map(statusText), charts.sales_funnel.values, palette.accent);
    createChart("document", "documentChart", "doughnut", charts.document_status.labels.map(statusText), charts.document_status.values, {
        border: palette.doughnutBorder,
        fill: palette.doughnutFill,
    });
}

function renderMiniList(containerId, labels, values, suffix = "KZT") {
    const container = document.getElementById(containerId);
    if (!labels.length) {
        container.innerHTML = emptyState("Данных пока нет.");
        return;
    }

    container.innerHTML = labels
        .map((label, index) => `
            <article class="mini-item">
                <div class="mini-item-top">
                    <strong>${escapeHtml(label)}</strong>
                    <span>${suffix ? `${formatCurrency(values[index])} ${suffix}` : escapeHtml(values[index])}</span>
                </div>
            </article>
        `)
        .join("");
}

function renderInsights() {
    const container = document.getElementById("insightsPanel");
    if (!state.dashboard.insights.length) {
        container.innerHTML = emptyState("Инсайты появятся, когда в системе будет больше активности.");
        return;
    }

    container.innerHTML = state.dashboard.insights
        .map((item) => `
            <article class="insight-card tone-${escapeHtml(item.tone)}">
                <span>${escapeHtml(item.title)}</span>
                <strong>${escapeHtml(item.value)}</strong>
                <p>${escapeHtml(item.description)}</p>
            </article>
        `)
        .join("");
}

function renderDueDocuments() {
    const container = document.getElementById("dueDocuments");
    if (!state.dashboard.due_documents.length) {
        container.innerHTML = emptyState("Нет документов, требующих срочного внимания.");
        return;
    }

    container.innerHTML = state.dashboard.due_documents
        .map((item) => `
            <article class="mini-item">
                <div class="mini-item-top">
                    <strong>${escapeHtml(item.title)}</strong>
                    ${statusBadge(item.status)}
                </div>
                <div class="meta-row">${escapeHtml(item.client_name)}</div>
                <div class="meta-row">${escapeHtml(item.due_date)} · ${escapeHtml(formatDaysLeft(item.days_left))}</div>
                <div class="action-row">
                    ${hasPermission("can_send_email") ? `<button class="action-button" type="button" data-action="select-document-email" data-id="${item.id}">Открыть письмо</button>` : ""}
                    ${hasPermission("can_manage_documents") ? `<button class="action-button" type="button" data-action="approve-document" data-id="${item.id}">Согласовать</button>` : ""}
                </div>
            </article>
        `)
        .join("");
}

function renderTeamLoad() {
    const container = document.getElementById("teamLoad");
    if (!state.dashboard.team_load.length) {
        container.innerHTML = emptyState("Командная загрузка появится после добавления задач.");
        return;
    }

    container.innerHTML = state.dashboard.team_load
        .map((item) => `
            <article class="mini-item">
                <div class="mini-item-top">
                    <strong>${escapeHtml(item.owner)}</strong>
                    <span>${item.high_priority} приор.</span>
                </div>
                <div class="meta-row">Открыто: ${item.open_count} · В работе: ${item.in_progress_count}</div>
            </article>
        `)
        .join("");
}

function renderClientHealth() {
    const container = document.getElementById("clientsHealthPanel");
    if (!state.dashboard.client_health.length) {
        container.innerHTML = emptyState("Пока недостаточно данных для оценки клиентов.");
        return;
    }

    container.innerHTML = state.dashboard.client_health
        .map((item) => `
            <article class="health-card tone-${escapeHtml(item.tone)}">
                <div class="health-top">
                    <div>
                        <strong>${escapeHtml(item.name)}</strong>
                        <div class="meta-row">${formatCurrency(item.revenue)} KZT</div>
                    </div>
                    <div class="score-ring">
                        <span>${item.score}</span>
                    </div>
                </div>
                <div class="meta-row">Документы в работе: ${item.waiting_documents} · Активные задачи: ${item.open_tasks}</div>
                <p>${escapeHtml(item.next_step)}</p>
            </article>
        `)
        .join("");
}

function renderPipelineBoard() {
    const container = document.getElementById("pipelineBoard");
    if (!state.dashboard.stage_board.length) {
        container.innerHTML = emptyState("Воронка пуста.");
        return;
    }

    container.innerHTML = state.dashboard.stage_board
        .map((column) => `
            <section class="stage-column">
                <header class="stage-head">
                    <div>
                        <strong>${escapeHtml(column.label)}</strong>
                        <div class="meta-row">${column.count} сделок</div>
                    </div>
                    <span>${formatCurrency(column.total)} KZT</span>
                </header>
                <div class="stage-items">
                    ${column.items.length
                        ? column.items.map((item) => `
                            <article class="stage-item">
                                <strong>${escapeHtml(item.title)}</strong>
                                <div class="meta-row">${escapeHtml(item.client_name)}</div>
                                <div class="meta-row">${escapeHtml(item.owner || "Без ответственного")} · ${escapeHtml(item.region || "Без региона")}</div>
                                <div class="stage-bottom">
                                    <span>${formatCurrency(item.total)} KZT</span>
                                    ${column.status !== "won" && hasPermission("can_manage_sales")
                                        ? `<button class="action-button" type="button" data-action="advance-sale" data-id="${item.id}" data-status="${column.status}">Продвинуть</button>`
                                        : `<span class="success-label">${column.status === "won" ? "Сделка закрыта" : "Только просмотр"}</span>`}
                                </div>
                            </article>
                        `).join("")
                        : emptyState("Пусто")}
                </div>
            </section>
        `)
        .join("");
}

function renderTemplates() {
    const container = document.getElementById("documentTemplates");
    const templates = state.dashboard.document_templates || [];
    if (!templates.length) {
        container.innerHTML = emptyState("Шаблоны пока не подготовлены.");
        return;
    }

    container.innerHTML = templates
        .map((template) => `
            <article class="template-card">
                <span class="template-type">${escapeHtml(statusText(template.type))}</span>
                <strong>${escapeHtml(template.title)}</strong>
                <p>${escapeHtml(template.description)}</p>
                <div class="meta-row">${formatCurrency(template.amount)} KZT</div>
                ${hasPermission("can_manage_documents")
                    ? `<button class="secondary-button" type="button" data-action="apply-template" data-template="${escapeHtml(template.id)}">Подставить в форму</button>`
                    : `<div class="meta-row">Шаблон доступен для просмотра</div>`}
            </article>
        `)
        .join("");
}

function renderClients() {
    const query = document.getElementById("clientFilter").value.trim().toLowerCase();
    const status = document.getElementById("clientStatusFilter").value;
    const items = state.dashboard.clients.filter((client) => {
        const haystack = [client.name, client.company, client.email, client.city, client.tags, client.notes]
            .join(" ")
            .toLowerCase();
        return (!query || haystack.includes(query)) && (status === "all" || client.status === status);
    });

    const container = document.getElementById("clientsTable");
    if (!items.length) {
        container.innerHTML = `<tr><td colspan="4">${emptyState("Клиенты не найдены.")}</td></tr>`;
        return;
    }

    container.innerHTML = items
        .map((client) => `
            <tr>
                <td>
                    <strong>${escapeHtml(client.name)}</strong>
                    <div class="meta-row">${escapeHtml(client.company || "")}</div>
                    <div class="meta-row">${escapeHtml(client.email || "")}</div>
                </td>
                <td>
                    <strong>${escapeHtml(client.segment || "Не указан")}</strong>
                    <div class="meta-row">${escapeHtml(client.city || "Без города")}</div>
                </td>
                <td>${statusBadge(client.status)}</td>
                <td>
                    <strong>${formatCurrency(client.revenue)} KZT</strong>
                    <div class="meta-row">${escapeHtml(client.tags || "Без тегов")}</div>
                </td>
            </tr>
        `)
        .join("");
}

function renderSales() {
    const selectedStatus = document.getElementById("salesStatusFilter").value;
    const items = state.dashboard.sales.filter((sale) => selectedStatus === "all" || sale.status === selectedStatus);
    const container = document.getElementById("salesTable");

    if (!items.length) {
        container.innerHTML = `<tr><td colspan="4">${emptyState("Сделки не найдены.")}</td></tr>`;
        return;
    }

    container.innerHTML = items
        .map((sale) => `
            <tr>
                <td>
                    <strong>${escapeHtml(sale.product_name)}</strong>
                    <div class="meta-row">${escapeHtml(sale.category)}</div>
                </td>
                <td>
                    <strong>${escapeHtml(sale.client_name)}</strong>
                    <div class="meta-row">${escapeHtml(sale.owner || "Без ответственного")} · ${escapeHtml(sale.region || "Без региона")}</div>
                </td>
                <td>
                    ${statusBadge(sale.status)}
                    ${sale.status !== "won" && hasPermission("can_manage_sales")
                        ? `<div class="inline-action"><button class="action-button" type="button" data-action="advance-sale" data-id="${sale.id}" data-status="${sale.status}">Следующий этап</button></div>`
                        : ""}
                </td>
                <td>
                    <strong>${formatCurrency(sale.total)} KZT</strong>
                    <div class="meta-row">${escapeHtml(sale.sale_date)}</div>
                </td>
            </tr>
        `)
        .join("");
}

function renderDocuments() {
    const query = document.getElementById("documentFilter").value.trim().toLowerCase();
    const selectedStatus = document.getElementById("documentStatusFilter").value;
    const items = state.dashboard.documents.filter((documentItem) => {
        const haystack = [documentItem.title, documentItem.client_name, documentItem.type, documentItem.content]
            .join(" ")
            .toLowerCase();
        return (!query || haystack.includes(query)) && (selectedStatus === "all" || documentItem.status === selectedStatus);
    });
    const container = document.getElementById("documentsTable");

    if (!items.length) {
        container.innerHTML = `<tr><td colspan="4">${emptyState("Документы не найдены.")}</td></tr>`;
        return;
    }

    container.innerHTML = items
        .map((item) => `
            <tr>
                <td>
                    <strong>${escapeHtml(item.title)}</strong>
                    <div class="meta-row">${escapeHtml(statusText(item.type))} · ${formatCurrency(item.amount)} KZT</div>
                </td>
                <td>
                    <strong>${escapeHtml(item.client_name)}</strong>
                    <div class="meta-row">${escapeHtml(item.client_email || "")}</div>
                </td>
                <td>${statusBadge(item.status)}</td>
                <td>
                    <div class="action-row">
                        <button class="action-button" type="button" data-action="download-pdf" data-id="${item.id}">PDF</button>
                        ${hasPermission("can_send_email")
                            ? `<button class="action-button" type="button" data-action="select-document-email" data-id="${item.id}">Письмо</button>`
                            : ""}
                        ${item.status !== "approved" && hasPermission("can_manage_documents")
                            ? `<button class="action-button" type="button" data-action="approve-document" data-id="${item.id}">Согласовать</button>`
                            : ""}
                    </div>
                </td>
            </tr>
        `)
        .join("");
}

function renderTasks() {
    const container = document.getElementById("taskList");
    if (!state.dashboard.tasks.length) {
        container.innerHTML = emptyState("Задачи пока не созданы.");
        return;
    }

    container.innerHTML = state.dashboard.tasks
        .map((task) => `
            <article class="task-item">
                <div class="task-top">
                    <div>
                        <strong>${escapeHtml(task.title)}</strong>
                        <div class="meta-row">${escapeHtml(task.client_name || "Без клиента")} · ${escapeHtml(task.owner || "Без ответственного")}</div>
                    </div>
                    <div class="task-badges">
                        ${statusBadge(task.priority)}
                        ${statusBadge(task.status)}
                    </div>
                </div>
                <p>${escapeHtml(task.description || "Описание не заполнено.")}</p>
                <div class="task-bottom">
                    <span>Срок: ${escapeHtml(task.due_date || "не указан")}</span>
                    <div class="action-row">
                        ${task.status !== "in_progress" && hasPermission("can_manage_tasks") ? `<button class="action-button" type="button" data-action="task-progress" data-id="${task.id}">В работу</button>` : ""}
                        ${task.status !== "done" && hasPermission("can_manage_tasks") ? `<button class="action-button" type="button" data-action="task-done" data-id="${task.id}">Готово</button>` : ""}
                    </div>
                </div>
            </article>
        `)
        .join("");
}

function renderActivity() {
    const activityContainer = document.getElementById("activityFeed");
    const emailContainer = document.getElementById("emailLogs");

    activityContainer.innerHTML = state.dashboard.activity.length
        ? state.dashboard.activity
            .map((item) => `
                <article class="activity-item">
                    <div class="activity-top">
                        <strong>${escapeHtml(item.label)}</strong>
                        ${statusBadge(item.meta)}
                    </div>
                    <div class="meta-row">${escapeHtml(item.type)} · ${escapeHtml(item.moment)}</div>
                </article>
            `)
            .join("")
        : emptyState("Активность появится после действий в системе.");

    emailContainer.innerHTML = state.dashboard.email_logs.length
        ? state.dashboard.email_logs
            .map((item) => `
                <article class="activity-item">
                    <div class="activity-top">
                        <strong>${escapeHtml(item.subject)}</strong>
                        ${statusBadge(item.status)}
                    </div>
                    <div class="meta-row">${escapeHtml(item.recipient)} · ${escapeHtml(item.document_title || "Без документа")}</div>
                </article>
            `)
            .join("")
        : emptyState("Письма пока не отправлялись.");
}

function renderSearchResults(items) {
    const container = document.getElementById("searchResults");
    if (!items.length) {
        container.innerHTML = emptyState("По запросу ничего не найдено.");
        return;
    }

    container.innerHTML = items
        .map((item) => `
            <article class="result-card">
                <div class="result-top">
                    <strong>${escapeHtml(item.title)}</strong>
                    ${statusBadge(item.type)}
                </div>
                <div>${escapeHtml(item.subtitle || "")}</div>
                <div class="meta-row">${escapeHtml(item.meta || "")}</div>
            </article>
        `)
        .join("");
}

function renderSelectOptions() {
    const sortedClients = [...state.dashboard.clients].sort((a, b) => a.name.localeCompare(b.name, "ru"));
    const clientOptions = sortedClients
        .map((client) => `<option value="${client.id}">${escapeHtml(client.name)} (${escapeHtml(client.company || client.email)})</option>`)
        .join("");

    const saleClientSelect = document.getElementById("saleClientSelect");
    const documentClientSelect = document.getElementById("documentClientSelect");
    const taskClientSelect = document.getElementById("taskClientSelect");
    const chatClientSelect = document.getElementById("chatClientSelect");

    if (saleClientSelect) {
        saleClientSelect.innerHTML = clientOptions;
    }
    if (documentClientSelect) {
        documentClientSelect.innerHTML = clientOptions;
    }
    if (taskClientSelect) {
        taskClientSelect.innerHTML = `<option value="">Без привязки</option>${clientOptions}`;
    }
    if (chatClientSelect) {
        chatClientSelect.innerHTML = hasPermission("client_scoped")
            ? clientOptions
            : `<option value="">Без привязки</option>${clientOptions}`;
    }

    const roomSelect = document.getElementById("chatRoomSelect");
    roomSelect.innerHTML = state.dashboard.rooms
        .map((room) => `<option value="${escapeHtml(room.id)}">${escapeHtml(room.label)}</option>`)
        .join("");
    roomSelect.value = state.currentRoom;
}

async function loadChat(room = state.currentRoom) {
    state.currentRoom = room;
    const data = await api(`/api/chat/messages?room=${encodeURIComponent(room)}`);
    const container = document.getElementById("chatFeed");

    if (!data.items.length) {
        container.innerHTML = emptyState("В этой комнате пока нет сообщений.");
        return;
    }

    container.innerHTML = data.items
        .map((item) => `
            <article class="chat-bubble ${escapeHtml(item.sender_role)}">
                <div class="chat-meta">
                    <strong>${escapeHtml(item.sender_name)}</strong>
                    <span>${escapeHtml(item.created_at)}</span>
                </div>
                <div class="meta-row">${escapeHtml(item.client_name || ROOM_LABELS[item.room_name] || item.room_name)}</div>
                <p>${escapeHtml(item.message)}</p>
            </article>
        `)
        .join("");
    container.scrollTop = container.scrollHeight;
}

function prepareEmailComposer(documentId) {
    const emailForm = document.getElementById("emailForm");
    if (!emailForm) {
        showNotice("Отправка письма недоступна для вашего профиля.");
        return;
    }
    const documentItem = state.dashboard.documents.find((item) => Number(item.id) === Number(documentId));
    if (!documentItem) {
        showNotice("Документ не найден.");
        return;
    }

    setActivePanel("documents");
    document.getElementById("emailDocumentId").value = documentItem.id;
    document.getElementById("emailDocumentTitle").value = documentItem.title;
    document.getElementById("emailRecipient").value = documentItem.client_email || "";
    document.getElementById("emailSubject").value = `Согласование документа: ${documentItem.title}`;
    document.getElementById("emailBody").value =
        `Здравствуйте.\n\nНаправляем документ «${documentItem.title}» для согласования.\nПосле просмотра, пожалуйста, подтвердите документ по ссылке из письма.`;
    document.getElementById("emailPreview").textContent =
        "Форма письма подготовлена. После отправки система добавит ссылку на согласование и приложит PDF.";
    emailForm.scrollIntoView({ behavior: "smooth", block: "center" });
}

function applyTemplate(templateId) {
    const template = (state.dashboard.document_templates || []).find((item) => item.id === templateId);
    if (!template) {
        return;
    }
    const form = document.getElementById("documentForm");
    if (!form) {
        showNotice("В вашем профиле создание документов недоступно.");
        return;
    }

    setActivePanel("documents");
    document.querySelector('#documentForm [name="title"]').value = template.title;
    document.querySelector('#documentForm [name="type"]').value = template.type;
    document.querySelector('#documentForm [name="amount"]').value = template.amount;
    document.querySelector('#documentForm [name="content"]').value = template.content;
    form.scrollIntoView({ behavior: "smooth", block: "center" });
    showNotice("Шаблон подставлен в форму документа.");
}

function hydrateNotes() {
    const notes = readStorage(NOTE_STORAGE_KEY) || "";
    document.getElementById("quickNotes").value = notes;
}

function attachNotesAutosave() {
    const textarea = document.getElementById("quickNotes");
    textarea.addEventListener("input", () => {
        writeStorage(NOTE_STORAGE_KEY, textarea.value);
    });
}

function setDefaultDates() {
    const today = new Date();
    const plusWeek = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
    const formatDate = (value) => value.toISOString().slice(0, 10);

    const saleDate = document.querySelector('#saleForm [name="sale_date"]');
    const documentDate = document.querySelector('#documentForm [name="due_date"]');
    const taskDate = document.querySelector('#taskForm [name="due_date"]');

    if (saleDate && !saleDate.value) {
        saleDate.value = formatDate(today);
    }
    if (documentDate && !documentDate.value) {
        documentDate.value = formatDate(plusWeek);
    }
    if (taskDate && !taskDate.value) {
        taskDate.value = formatDate(plusWeek);
    }
}

async function updateTaskStatus(taskId, status) {
    const result = await api(`/api/tasks/${taskId}`, {
        method: "PATCH",
        body: JSON.stringify({ status }),
    });
    showNotice(result.message);
    await loadDashboard();
}

async function updateSaleStatus(saleId, currentStatus) {
    const nextIndex = SALE_FLOW.indexOf(currentStatus) + 1;
    const nextStatus = SALE_FLOW[nextIndex];
    if (!nextStatus) {
        showNotice("Эта сделка уже на финальном этапе.");
        return;
    }

    const result = await api(`/api/sales/${saleId}`, {
        method: "PATCH",
        body: JSON.stringify({ status: nextStatus }),
    });
    showNotice(result.message);
    await loadDashboard();
}

async function updateDocumentStatus(documentId, status) {
    const result = await api(`/api/documents/${documentId}`, {
        method: "PATCH",
        body: JSON.stringify({ status }),
    });
    showNotice(result.message);
    await loadDashboard();
}

async function loadDashboard() {
    state.dashboard = await api("/api/dashboard");
    state.session = state.dashboard.session || null;
    state.permissions = state.session && state.session.permissions ? state.session.permissions : {};
    renderSessionChrome();
    renderSpotlight();
    renderNotifications();
    renderQuickActions();
    renderSummary(state.dashboard.summary);
    renderInsights();
    renderDueDocuments();
    renderTeamLoad();
    renderCharts(state.dashboard.charts);
    renderMiniList("topClientsList", state.dashboard.charts.top_clients.labels, state.dashboard.charts.top_clients.values);
    renderMiniList("regionList", state.dashboard.charts.region_mix.labels, state.dashboard.charts.region_mix.values);
    renderClientHealth();
    renderPipelineBoard();
    renderTemplates();
    renderClients();
    renderSales();
    renderDocuments();
    renderTasks();
    renderActivity();
    renderSelectOptions();
    applyUserContext();
    await loadChat(state.currentRoom);
}

function attachWorkspaceNavigation() {
    if (!hasWorkspaceNavigation()) {
        return;
    }

    document.querySelectorAll("[data-workspace-tab]").forEach((button) => {
        button.addEventListener("click", () => {
            setActivePanel(button.dataset.workspaceTab);
        });
    });

    window.addEventListener("hashchange", () => {
        const nextPanel = window.location.hash.replace("#", "").trim() || "overview";
        setActivePanel(nextPanel, false);
    });
}

function attachAppearanceControls() {
    const savedTheme = normalizeTheme(document.documentElement.dataset.theme || readStorage(THEME_STORAGE_KEY));
    const savedSidebarState = (document.documentElement.dataset.sidebarState || readStorage(SIDEBAR_STORAGE_KEY)) === "collapsed";

    applyTheme(savedTheme, { skipChartRefresh: true });
    setSidebarCollapsed(savedSidebarState);

    document.querySelectorAll("[data-theme-option]").forEach((button) => {
        button.addEventListener("click", () => {
            applyTheme(button.dataset.themeOption);
            showNotice(`Тема «${button.textContent.trim()}» включена.`);
        });
    });

    document.querySelectorAll("[data-toggle-sidebar]").forEach((button) => {
        button.addEventListener("click", () => {
            setSidebarCollapsed(!state.sidebarCollapsed);
        });
    });
}

function attachFilters() {
    document.getElementById("clientFilter").addEventListener("input", renderClients);
    document.getElementById("clientStatusFilter").addEventListener("change", renderClients);
    document.getElementById("salesStatusFilter").addEventListener("change", renderSales);
    document.getElementById("documentFilter").addEventListener("input", renderDocuments);
    document.getElementById("documentStatusFilter").addEventListener("change", renderDocuments);
    document.getElementById("chatRoomSelect").addEventListener("change", async (event) => {
        await loadChat(event.target.value);
    });
}

function attachDelegatedActions() {
    document.addEventListener("click", async (event) => {
        const button = event.target.closest("[data-action]");
        if (!button) {
            return;
        }

        const action = button.dataset.action;
        const itemId = button.dataset.id;

        try {
            if (action === "jump-panel") {
                setActivePanel(button.dataset.panel || "overview");
                focusTarget(button.dataset.focus || "");
                return;
            }
            if (action === "download-pdf") {
                window.open(`/api/documents/${itemId}/pdf`, "_blank");
                return;
            }
            if (action === "select-document-email") {
                prepareEmailComposer(itemId);
                return;
            }
            if (action === "approve-document") {
                await updateDocumentStatus(itemId, "approved");
                return;
            }
            if (action === "advance-sale") {
                await updateSaleStatus(itemId, button.dataset.status);
                return;
            }
            if (action === "task-progress") {
                await updateTaskStatus(itemId, "in_progress");
                return;
            }
            if (action === "task-done") {
                await updateTaskStatus(itemId, "done");
                return;
            }
            if (action === "apply-template") {
                applyTemplate(button.dataset.template);
            }
        } catch (error) {
            showNotice(error.message);
        }
    });
}

function attachForms() {
    const clientForm = document.getElementById("clientForm");
    const saleForm = document.getElementById("saleForm");
    const documentForm = document.getElementById("documentForm");
    const emailForm = document.getElementById("emailForm");
    const taskForm = document.getElementById("taskForm");
    const chatForm = document.getElementById("chatForm");
    const searchForm = document.getElementById("searchForm");

    if (clientForm) {
        clientForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            try {
                const result = await api("/api/clients", {
                    method: "POST",
                    body: JSON.stringify(formToObject(event.currentTarget)),
                });
                event.currentTarget.reset();
                showNotice(result.message);
                await loadDashboard();
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (saleForm) {
        saleForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            try {
                const result = await api("/api/sales", {
                    method: "POST",
                    body: JSON.stringify(formToObject(event.currentTarget)),
                });
                event.currentTarget.reset();
                setDefaultDates();
                applyUserContext();
                showNotice(result.message);
                await loadDashboard();
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (documentForm) {
        documentForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            try {
                const result = await api("/api/documents", {
                    method: "POST",
                    body: JSON.stringify(formToObject(event.currentTarget)),
                });
                event.currentTarget.reset();
                setDefaultDates();
                showNotice(result.message);
                await loadDashboard();
                if (document.getElementById("emailForm")) {
                    prepareEmailComposer(result.document.id);
                }
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (emailForm) {
        emailForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const payload = formToObject(event.currentTarget);
            if (!payload.document_id) {
                showNotice("Сначала выберите документ.");
                return;
            }
            try {
                const result = await api(`/api/documents/${payload.document_id}/email`, {
                    method: "POST",
                    body: JSON.stringify(payload),
                });
                document.getElementById("emailPreview").textContent =
                    result.delivery_status === "demo-preview"
                        ? `SMTP пока не настроен, поэтому показан безопасный режим предпросмотра.\n\n${result.preview}\n\nСсылка: ${result.approval_url}`
                        : `Письмо отправлено. Ссылка на согласование: ${result.approval_url}`;
                showNotice(result.message);
                await loadDashboard();
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (taskForm) {
        taskForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            try {
                const result = await api("/api/tasks", {
                    method: "POST",
                    body: JSON.stringify(formToObject(event.currentTarget)),
                });
                event.currentTarget.reset();
                setDefaultDates();
                applyUserContext();
                showNotice(result.message);
                await loadDashboard();
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (chatForm) {
        chatForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const payload = formToObject(event.currentTarget);
            payload.room_name = state.currentRoom;
            try {
                const result = await api("/api/chat/messages", {
                    method: "POST",
                    body: JSON.stringify(payload),
                });
                const messageField = chatForm.querySelector('[name="message"]');
                if (messageField) {
                    messageField.value = "";
                }
                showNotice(result.message);
                await loadChat(state.currentRoom);
                await loadDashboard();
            } catch (error) {
                showNotice(error.message);
            }
        });
    }

    if (searchForm) {
        searchForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const query = document.getElementById("searchInput").value.trim();
            const scope = document.getElementById("searchScope").value;
            if (!query) {
                renderSearchResults([]);
                return;
            }
            try {
                const result = await api(`/api/search?q=${encodeURIComponent(query)}&scope=${encodeURIComponent(scope)}`);
                renderSearchResults(result.items);
            } catch (error) {
                showNotice(error.message);
            }
        });
    }
}

async function init() {
    attachAppearanceControls();
    setDefaultDates();
    hydrateNotes();
    attachNotesAutosave();
    attachWorkspaceNavigation();
    attachFilters();
    attachDelegatedActions();
    attachForms();
    await loadDashboard();
    const initialPanel = window.location.hash.replace("#", "").trim() || "overview";
    setActivePanel(initialPanel, false);
    window.setInterval(() => {
        loadChat(state.currentRoom).catch(() => {});
    }, 8000);
}

document.addEventListener("DOMContentLoaded", () => {
    init().catch((error) => {
        showNotice(error.message || "Не удалось загрузить интерфейс.");
    });
});
