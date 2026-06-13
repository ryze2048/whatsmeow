// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var errProxyMissingPort = errors.New("proxy port is required when proxy host is set")

// BuildProxyURL 把 ImportRequest/LoginRequest 里的代理字段转换成可直接使用的代理 URL。
// 如果 ProxyURL 已经是完整 URL，会直接返回；否则用 host/port/user/pass 组装。
func BuildProxyURL(req ImportRequest, defaultScheme string) (string, error) {
	if strings.TrimSpace(req.ProxyURL) != "" {
		return strings.TrimSpace(req.ProxyURL), nil
	}
	proxy := ProxyConfig{
		Scheme: defaultScheme,
		Host:   req.ProxyHost,
		Port:   req.ProxyPort,
		User:   req.ProxyUser,
		Pass:   req.ProxyPass,
	}
	return proxy.URL()
}

// ProxyConfig 是 Manager 内部使用的标准代理配置结构。
// 业务层可以传完整 URL，也可以传拆分字段，最终都会归一化成 URL。
type ProxyConfig struct {
	Scheme string
	Host   string
	Port   string
	User   string
	Pass   string
}

func (p ProxyConfig) URL() (string, error) {
	p.Scheme = strings.TrimSpace(p.Scheme)
	p.Host = strings.TrimSpace(p.Host)
	p.Port = strings.TrimSpace(p.Port)
	p.User = strings.TrimSpace(p.User)
	p.Pass = strings.TrimSpace(p.Pass)

	if p.Host == "" {
		return "", nil
	}
	if p.Port == "" {
		return "", errProxyMissingPort
	}
	if p.Scheme == "" {
		p.Scheme = defaultProxyScheme
	}
	// 这里提前校验 scheme，是为了让错误代理配置尽早失败。
	// 否则可能等到 websocket dial 或 TLS 阶段才超时，排查成本更高。
	if err := validateProxyScheme(p.Scheme); err != nil {
		return "", err
	}

	out := url.URL{
		Scheme: p.Scheme,
		Host:   net.JoinHostPort(p.Host, p.Port),
	}
	if p.User != "" || p.Pass != "" {
		out.User = url.UserPassword(p.User, p.Pass)
	}
	return out.String(), nil
}

func validateProxyScheme(scheme string) error {
	switch strings.ToLower(scheme) {
	case "http", "https", "socks5":
		return nil
	default:
		return fmt.Errorf("unsupported proxy scheme %q", scheme)
	}
}
