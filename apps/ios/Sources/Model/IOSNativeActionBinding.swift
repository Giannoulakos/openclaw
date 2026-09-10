import Foundation
import OpenClawChatUI
import OpenClawKit
import OpenClawProtocol

struct IOSNativeActionBinding: Sendable {
    let session: OpenClawNativeSessionRef
    let gateway: GatewayNodeSession
    let route: GatewayNodeSessionRoute
    private let httpContext: GatewayAdmittedHTTPContext?

    init(
        session: OpenClawNativeSessionRef,
        gateway: GatewayNodeSession,
        route: GatewayNodeSessionRoute,
        httpContext: GatewayAdmittedHTTPContext? = nil)
    {
        self.session = session
        self.gateway = gateway
        self.route = route
        self.httpContext = httpContext
    }

    static func capture(
        session: OpenClawNativeSessionRef,
        gateway: GatewayNodeSession,
        route: GatewayNodeSessionRoute) async throws -> Self
    {
        let binding = Self(session: session, gateway: gateway, route: route)
        try await binding.requireAvailable()
        guard let context = await gateway.admittedHTTPContext(ifCurrentRoute: route),
              await binding.isCurrent()
        else { throw OpenClawNativeActionError(Self.unavailableReason) }
        return Self(session: session, gateway: gateway, route: route, httpContext: context)
    }

    var mediaConnection: IOSMediaArtifactLoader.Connection? {
        self.httpContext.map { IOSMediaArtifactLoader.Connection(binding: self, context: $0) }
    }

    var expectedProfileId: String {
        self.session.owner.profileID
    }

    static let unavailableReason = "The selected account or Gateway connection changed. Open the session again."

    func isCurrent() async -> Bool {
        await self.gateway.currentRoute(ifGatewayID: self.session.owner.gatewayID) == self.route
    }

    func requireAvailable() async throws {
        guard await self.isCurrent() else { throw GatewayNodeSessionRequestError.routeChangedBeforeDispatch }
        let supported = await self.gateway.supportsServerCapability(.profileBinding, ifCurrentRoute: self.route)
        guard await self.isCurrent() else { throw GatewayNodeSessionRequestError.routeChangedBeforeDispatch }
        guard supported == true else {
            throw OpenClawNativeActionError("Update the selected Gateway to use account-bound native actions.")
        }
    }

    func request(_ request: OpenClawChatGatewayRequest) async throws -> Data {
        try await self.requireAvailable()
        let data = try await self.gateway.request(
            request,
            ifCurrentRoute: self.route,
            distinguishPreDispatchRouteChange: true,
            expectedProfileId: self.expectedProfileId)
        // Dispatch already happened. A lost route here is not evidence that a
        // mutation was never sent; retain the caller's uncertain-result handling.
        guard await self.isCurrent() else { throw CancellationError() }
        return data
    }

    func accepts(_ event: EventFrame) async -> Bool {
        guard event.recipientprofileid?.utf8.elementsEqual(self.expectedProfileId.utf8) == true else { return false }
        return await self.isCurrent()
    }

    func matches(_ other: Self) -> Bool {
        self.session == other.session && self.gateway === other.gateway && self.route == other.route
    }

    static func isProfileMismatch(_ error: Error) -> Bool {
        (error as? GatewayResponseError)?.detailsReason == "EXPECTED_PROFILE_MISMATCH"
    }

    func request(method: String, paramsJSON: String?, timeoutSeconds: Int) async throws -> Data {
        let params = try paramsJSON.map {
            try JSONDecoder().decode([String: OpenClawProtocol.AnyCodable].self, from: Data($0.utf8))
        } ?? [:]
        return try await self.request(.init(
            method: method, params: params, timeoutMs: Double(timeoutSeconds) * 1000))
    }
}
