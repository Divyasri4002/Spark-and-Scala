class JobMarketDashboard {
    constructor() {
        this.baseUrl = 'http://localhost:5000/api';
        this.charts = {};
        this.init();
    }

    async init() {
        // Start by wiring up UI controls (cover, modal, search)
        this.wireUI();

        // Load dataset summary immediately so header counts appear
        await this.loadDatasetInfo();
        // Show home section by default
        this.showSection('home');
        // Do not load all charts until user selects them to save startup time
        // but prepare the skills chart container in background
        this.createSkillsChart(); // warm-up skills chart
    }

    wireUI() {
        // Explore button: go directly to dashboard
        const exploreBtn = document.getElementById('exploreBtn');
        if (exploreBtn) {
            exploreBtn.addEventListener('click', () => {
                this.showSection('dashboard');
                // Load all default visualizations
                this.createDomainChart();
                this.createSkillsChart();
                this.createCompanyChart();
                this.createExperienceChart();
                this.createLocationChart();
                this.createSalaryChart();
            });
        }

        // Global search behavior
        const search = document.getElementById('globalSearch');
        if (search) search.addEventListener('input', (ev) => this.performSearch(ev.target.value));

        // Navigation links
        document.querySelectorAll('.nav-link').forEach(a => {
            a.addEventListener('click', (e) => {
                e.preventDefault();
                const route = a.getAttribute('data-route');
                this.showSection(route);
                // update active state
                document.querySelectorAll('.nav-link').forEach(n => n.classList.remove('active'));
                a.classList.add('active');
            });
        });
    }

    async fetchData(endpoint) {
        try {
            const response = await fetch(`${this.baseUrl}/${endpoint}`);
            return await response.json();
        } catch (error) {
            console.error(`Error fetching ${endpoint}:`, error);
            return [];
        }
    }

    async loadDatasetInfo() {
        const info = await this.fetchData('dataset-info');
        document.getElementById('totalJobs').textContent = info.total_jobs || '-';
        document.getElementById('totalDomains').textContent = info.unique_domains || '-';
        document.getElementById('totalCompanies').textContent = info.unique_companies || '-';
        document.getElementById('totalLocations').textContent = info.unique_locations || '-';
        // Populate a short summary insight
        const summaryEl = document.getElementById('insightsSummary');
        if (info && info.total_jobs) {
            summaryEl.textContent = `This dataset contains ${info.total_jobs} job postings across ${info.unique_domains} domains and ${info.unique_companies} companies in ${info.unique_locations} locations.`;
        } else {
            summaryEl.textContent = 'Dataset information not available yet.';
        }
    }

    async loadAllCharts() {
        await this.createDomainChart();
        await this.createSkillsChart();
        await this.createCompanyChart();
        await this.createExperienceChart();
        await this.createLocationChart();
        await this.createSalaryChart();
    }

    async createDomainChart() {
        // ensure container visible and charts loaded
        const container = document.getElementById('domainContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('domain-analysis');
        const ctx = document.getElementById('domainChart').getContext('2d');
        
        this.charts.domain = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(item => item.domain),
                datasets: [{
                    label: 'Number of Jobs',
                    data: data.map(item => item.job_count),
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    async createSkillsChart() {
        const container = document.getElementById('skillsContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('skill-demand');
        const ctx = document.getElementById('skillsChart').getContext('2d');
        
        this.charts.skills = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.map(item => item.skill),
                datasets: [{
                    data: data.map(item => item.demand_count),
                    backgroundColor: [
                        '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                        '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF'
                    ]
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'right'
                    }
                }
            }
        });

        // Fill top skills list (top 8)
        const skillsList = document.getElementById('topSkillsList');
        skillsList.innerHTML = '';
        data.slice(0, 8).forEach(item => {
            const li = document.createElement('li');
            li.textContent = `${item.skill} — ${item.demand_count} mentions`;
            skillsList.appendChild(li);
        });
    }

    async createCompanyChart() {
        const container = document.getElementById('companyContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('company-analysis');
        const ctx = document.getElementById('companyChart').getContext('2d');
        
        this.charts.company = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(item => item.company),
                datasets: [{
                    label: 'Jobs Posted',
                    data: data.map(item => item.job_count),
                    backgroundColor: 'rgba(255, 159, 64, 0.8)'
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // Populate company table top 10
        this.populateCompanyTable(data.slice(0, 10));
    }

    populateCompanyTable(companies) {
        const tbody = document.getElementById('companyTableBody');
        tbody.innerHTML = '';
        if (!companies || companies.length === 0) {
            const tr = document.createElement('tr');
            tr.innerHTML = '<td colspan="3">No data available</td>';
            tbody.appendChild(tr);
            return;
        }

        companies.forEach(c => {
            const tr = document.createElement('tr');
            const avgSalary = c.avg_salary ? Math.round(c.avg_salary) : '-';
            tr.innerHTML = `<td>${c.company || '—'}</td><td>${c.job_count}</td><td>${avgSalary}</td>`;
            tbody.appendChild(tr);
        });

        // Generate recommendations based on top companies and skills
        this.generateRecommendations(companies);
    }

    generateRecommendations(companies) {
        const rec = document.getElementById('recommendations');
        rec.innerHTML = '';
        if (!companies || companies.length === 0) {
            rec.innerHTML = '<li>No recommendations available</li>';
            return;
        }

        // Example recommendations - keep them general and professional
        const topCompany = companies[0];
        const items = [
            `Consider monitoring hiring activity at ${topCompany.company} (top employer in dataset).`,
            'Prioritize skills that appear frequently in the Top Skills list when designing training programs.',
            'Target outreach to cities with the largest job counts to increase placement success.',
            'Use average salary data to benchmark compensation for open roles and attract talent.'
        ];

        items.forEach(text => {
            const li = document.createElement('li');
            li.textContent = text;
            rec.appendChild(li);
        });
    }

    async createExperienceChart() {
        const container = document.getElementById('experienceContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('experience-trends');
        const ctx = document.getElementById('experienceChart').getContext('2d');
        
        this.charts.experience = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(item => item.experience_level),
                datasets: [
                    {
                        label: 'Average Salary (₹)',
                        data: data.map(item => item.avg_salary),
                        backgroundColor: 'rgba(75, 192, 192, 0.8)'
                    }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    async createLocationChart() {
        const container = document.getElementById('locationContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('location-analysis');
        const ctx = document.getElementById('locationChart').getContext('2d');
        
        this.charts.location = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: data.map(item => item.city),
                datasets: [{
                    data: data.map(item => item.job_count),
                    backgroundColor: [
                        '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                        '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF'
                    ]
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'right'
                    }
                }
            }
        });
    }

    async createSalaryChart() {
        const container = document.getElementById('salaryContainer');
        container.classList.remove('hidden');
        const data = await this.fetchData('salary-distribution');
        const salaries = data.map(item => item.avg_salary).filter(s => s > 0);
        const ctx = document.getElementById('salaryChart').getContext('2d');
        this.charts.salary = new Chart(ctx, {
            type: 'line',
            data: {
                labels: salaries.map((_, i) => i + 1),
                datasets: [{
                    label: 'Salary Distribution',
                    data: salaries,
                    borderColor: 'rgba(153, 102, 255, 1)',
                    backgroundColor: 'rgba(153, 102, 255, 0.1)',
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

function openSparkUI() {
    window.open('http://localhost:4040', '_blank');
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', () => {
    new JobMarketDashboard();
});

// Show or hide visualizations based on selection name
JobMarketDashboard.prototype.showVisualization = function(name) {
    // Map viz keys to container ids
    const vizMap = {
        domain: 'domainContainer',
        skills: 'skillsContainer',
        company: 'companyContainer',
        experience: 'experienceContainer',
        location: 'locationContainer',
        salary: 'salaryContainer',
        insights: null, // handled via insights section already visible
        companies: null
    };

    // Hide all chart containers first
    ['domainContainer','skillsContainer','companyContainer','experienceContainer','locationContainer','salaryContainer'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.add('hidden');
    });

    const target = vizMap[name];
    if (target) {
        // Load the chart if not already initialized
        switch(name) {
            case 'domain': this.createDomainChart(); break;
            case 'skills': this.createSkillsChart(); break;
            case 'company': this.createCompanyChart(); break;
            case 'experience': this.createExperienceChart(); break;
            case 'location': this.createLocationChart(); break;
            case 'salary': this.createSalaryChart(); break;
        }
        // ensure visible (create* removes hidden)
        const el = document.getElementById(target);
        if (el) el.classList.remove('hidden');
        // scroll into view
        el.scrollIntoView({behavior:'smooth', block:'start'});
    } else {
        // For insights and companies table, scroll to sections
        if (name === 'insights') {
            document.getElementById('insightsSummary').scrollIntoView({behavior:'smooth', block:'center'});
        }
        if (name === 'companies') {
            document.getElementById('companyTable').scrollIntoView({behavior:'smooth', block:'center'});
        }
    }
};

// Section navigation: show/hide site sections
JobMarketDashboard.prototype.showSection = function(name) {
    // hide all main sections
    document.querySelectorAll('main .section').forEach(s => s.classList.add('hidden'));
    const el = document.getElementById(name);
    if (el) {
        el.classList.remove('hidden');
        // when showing dashboard, ensure default charts/insights are loaded
        if (name === 'dashboard') {
            // load default charts if not already
            this.createSkillsChart();
            this.createCompanyChart();
        }
        el.scrollIntoView({behavior:'smooth', block:'start'});
    }
};

// Basic global search to filter companies table, top skills, and reveal charts by name
JobMarketDashboard.prototype.performSearch = function(query) {
    query = (query || '').toLowerCase().trim();
    // Filter company table rows
    const tbody = document.getElementById('companyTableBody');
    if (tbody) {
        Array.from(tbody.querySelectorAll('tr')).forEach(tr => {
            const text = tr.textContent.toLowerCase();
            tr.style.display = text.includes(query) ? '' : 'none';
        });
    }

    // Filter skills list
    const skills = document.getElementById('topSkillsList');
    if (skills) {
        Array.from(skills.children).forEach(li => {
            li.style.display = li.textContent.toLowerCase().includes(query) ? '' : 'none';
        });
    }

    // If query matches chart names, show them
    const chartNames = ['domain','skills','company','experience','location','salary'];
    chartNames.forEach(name => {
        if (name.includes(query) && query.length>1) {
            this.showVisualization(name);
        }
    });
};